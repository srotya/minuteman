/**
 * Copyright 2017 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.minuteman.wal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Logger;

/**
 * Write ahead log implementation with replication in mind with automated
 * compaction built-in. This WAL implementation has the concept of segments
 * where a segment represents a chunk to data that is successfully persisted.
 * 
 * Compaction implies that data replicated over by followers successfully can be
 * marked for deletion.
 * 
 * @author ambud
 */
public class MappedWAL implements WAL {

	private static final Logger logger = Logger.getLogger(MappedWAL.class.getName());
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private ReadLock read = lock.readLock();
	private WriteLock write = lock.writeLock();
	private MappedByteBuffer currentWrite;
	private int segmentSize;
	private volatile int segmentCounter;
	private RandomAccessFile raf;
	private String walDirectory;
	private Map<String, MappedWALFollower> followerMap;
	private volatile int counter;
	private int maxCounter;
	private int isrThreshold;
	private volatile int commitOffset;
	private AtomicBoolean disableWalDeletion;

	public MappedWAL() {
	}

	@Override
	public void configure(Map<String, String> conf, ScheduledExecutorService es) throws IOException {
		walDirectory = conf.getOrDefault(WAL_DIR, DEFAULT_WAL_DIR);
		logger.info("Configuring WAL directory:" + walDirectory);
		new File(walDirectory).mkdirs();
		// maximum size of a segment
		// each segment is memory mapped in whole; default's 100MB
		segmentSize = Integer.parseInt(conf.getOrDefault(WAL_SEGMENT_SIZE, String.valueOf(DEFAULT_WALL_SIZE)));
		logger.info("Configuring segment size:" + segmentSize);
		// maximum number of writes after which a force flush is triggered
		maxCounter = Integer.parseInt(conf.getOrDefault(WAL_SEGMENT_FLUSH_COUNT, String.valueOf(-1)));
		logger.info("Configuring max flush counter to:" + maxCounter);
		// offset threshold beyond which a follower is not considered an ISR
		isrThreshold = Integer.parseInt(conf.getOrDefault(WAL_ISR_THRESHOLD, String.valueOf(1024 * 1024 * 8)));
		// create an empty map of all followers
		followerMap = new ConcurrentHashMap<>();
		// disable wal deleteion i.e. all WALs will be preserved even after it
		// is
		// read by the follower
		disableWalDeletion = new AtomicBoolean(Boolean.parseBoolean(conf.getOrDefault(WAL_DELETION_DISABLED, "false")));
		// isr polling thread
		es.scheduleAtFixedRate(() -> {
			for (Entry<String, MappedWALFollower> entry : followerMap.entrySet()) {
				MappedWALFollower follower = entry.getValue();
				String followerId = entry.getKey();
				if (segmentCounter != follower.getFileId()
						|| currentWrite.position() - follower.getOffset() > isrThreshold) {
					follower.setIsr(false);
					logger.fine("Follower no longer an ISR:" + followerId);
				} else {
					follower.setIsr(true);
					logger.fine("Follower now an ISR:" + followerId);
				}
			}
		}, Integer.parseInt(conf.getOrDefault(WAL_ISRCHECK_DELAY, "1")),
				Integer.parseInt(conf.getOrDefault(WAL_ISRCHECK_FREQUENCY, "10")), TimeUnit.SECONDS);
		// initialize the wal segments
		checkAndRotateSegment(0);
	}

	@Override
	public void flush() throws IOException {
		write.lock();
		if (currentWrite != null) {
			logger.fine("Flushing buffer to disk");
			currentWrite.force();
		}
		write.unlock();
	}

	@Override
	public void close() throws IOException {
		write.lock();
		flush();
		if (raf != null) {
			raf.close();
		}
		for (Entry<String, MappedWALFollower> entry : followerMap.entrySet()) {
			if (entry.getValue().getReader() != null) {
				entry.getValue().getReader().close();
			}
		}
		write.unlock();
	}

	@Override
	public int getOffset() throws IOException {
		if (currentWrite != null) {
			return currentWrite.position();
		} else {
			return -1;
		}
	}

	@Override
	public void write(byte[] data, boolean flush) throws IOException {
		checkAndRotateSegment(data.length);
		write.lock();
		try {
			counter++;
			currentWrite.put(data);
			currentWrite.putInt(0, currentWrite.position());
			if (maxCounter != -1 && (flush || counter == maxCounter)) {
				flush();
				counter = 0;
			}
			int minimumOffset = getMinimumOffset();
			if (minimumOffset != Integer.MAX_VALUE) {
				commitOffset = minimumOffset;
			}
		} finally {
			write.unlock();
		}
	}

	@Override
	public WALRead read(String followerId, int offset, int maxBytes, int fileId) throws IOException {
		read.lock();
		MappedWALFollower follower = followerMap.get(followerId);
		if (follower == null) {
			follower = new MappedWALFollower();
			followerMap.put(followerId, follower);
			logger.info("No follower entry, creating one for:" + followerId);
		}
		read.unlock();
		WALRead readData = new WALRead();
		if (fileId != follower.getFileId()) {
			logger.fine("Follower file(" + follower.getFileId() + ") is doesn't match requested file id(" + fileId
					+ "), reseting buffer & file id");
			follower.setBuf(null);
			follower.setFileId(fileId);
			if (follower.getReader() != null) {
				logger.fine("Follower(" + followerId + ") has existing open file, now closing");
				follower.getReader().close();
			}
			deleteWALSegments();
		}
		if (follower.getBuf() == null) {
			File file = new File(getSegmentFileName(walDirectory, fileId));
			if (!file.exists() && fileId < segmentCounter) {
				logger.fine("Follower(" + followerId + ") requested file(" + fileId + "/" + file.getAbsolutePath()
						+ ") doesn't exist, incrementing");
				readData.setFileId(fileId + 1);
				readData.setNextOffset(0);
				return readData;
			}
			logger.fine("Follower(" + followerId + "), opening new wal segment to read:" + file.getAbsolutePath());
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			follower.setReader(raf);
			follower.setBuf(raf.getChannel().map(MapMode.READ_ONLY, 0, file.length()));
			follower.getBuf().getInt();
		}
		follower.setOffset(offset);
		int pos = follower.getBuf().getInt(0);
		ByteBuffer buf = follower.getBuf();
		buf.position(offset);
		if (offset < pos) {
			int length = maxBytes < (pos - offset) ? maxBytes : (pos - offset);
			byte[] dst = new byte[length];
			buf.get(dst);
			readData.setData(dst);
		}
		if (pos == buf.position() && segmentCounter > follower.getFileId()) {
			int newSegment = follower.getFileId() + 1;
			logger.fine("Follower(" + followerId
					+ ") doesn't have any more data to read from this segment, incrementing segment(" + newSegment
					+ "); segmentCounter:" + segmentCounter);
			readData.setFileId(newSegment);
			readData.setNextOffset(4);
		} else {
			readData.setFileId(fileId);
			readData.setNextOffset(buf.position());
		}
		write.lock();
		int minimumOffset = getMinimumOffset();
		if (minimumOffset != Integer.MAX_VALUE) {
			commitOffset = minimumOffset;
		}
		write.unlock();
		readData.setCommitOffset(commitOffset);
		return readData;
	}

	private void checkAndRotateSegment(int size) throws IOException {
		if (currentWrite == null || currentWrite.remaining() < size) {
			write.lock();
			if (raf != null) {
				logger.fine("Flushing current write buffer");
				currentWrite.force();
				logger.fine("Closing write access to current segment file:"
						+ getSegmentFileName(walDirectory, segmentCounter));
				raf.close();
			}
			boolean forward = false;
			if (currentWrite == null) {
				segmentCounter = getLastSegmentCounter(walDirectory);
				if (segmentCounter > 1) {
					forward = true;
				}
			} else {
				segmentCounter++;
			}
			raf = new RandomAccessFile(new File(getSegmentFileName(walDirectory, segmentCounter)), "rwd");
			currentWrite = raf.getChannel().map(MapMode.READ_WRITE, 0, segmentSize);
			if (forward) {
				// if this segment file already exists then forward the cursor
				// to the
				// latest committed location;
				int offset = currentWrite.getInt(0);
				currentWrite.position(offset);
				logger.info("Found existing WAL:" + getSegmentFileName(walDirectory, segmentCounter)
						+ ", forwarding offset to:" + offset);
			} else {
				// else write a blank 0 offset to the beginning of this brand
				// new buffer
				currentWrite.putInt(0);
			}
			logger.fine("Rotating segment file:" + getSegmentFileName(walDirectory, segmentCounter));
			write.unlock();
		}
	}

	public static String getSegmentFileName(String walDirectory, int segmentCounter) {
		return walDirectory + "/" + String.format("%012d", segmentCounter) + ".wal";
	}

	public static int getLastSegmentCounter(String walDirectory) {
		File[] files = new File(walDirectory).listFiles();
		if (files.length > 0) {
			return Integer.parseInt(files[files.length - 1].getName().replace(".wal", ""));
		} else {
			return 1;
		}
	}

	@Override
	public int getSegmentCounter() {
		return segmentCounter;
	}

	@Override
	public int getCurrentOffset() {
		return currentWrite.position();
	}

	@Override
	public int getCommitOffset() {
		return commitOffset;
	}

	@Override
	public void setCommitOffset(int commitOffset) {
		write.lock();
		this.commitOffset = commitOffset;
		write.unlock();
	}

	@Override
	public int getFollowerOffset(String followerId) {
		MappedWALFollower mappedWALFollower = followerMap.get(followerId);
		if (mappedWALFollower != null) {
			return mappedWALFollower.getOffset();
		}
		return -1;
	}

	private int getMinimumOffset() {
		String[] array = followerMap.keySet().toArray(new String[0]);
		int offsetMarker = Integer.MAX_VALUE;
		for (String tracker : array) {
			MappedWALFollower follower = followerMap.get(tracker);
			if (!follower.isIsr()) {
				continue;
			}
			int offset = follower.getOffset();
			if (offset < offsetMarker) {
				offsetMarker = offset;
			}
		}
		return offsetMarker;
	}

	@Override
	public void setWALDeletion(boolean status) {
		disableWalDeletion.set(status);
	}

	/**
	 * Delete WAL segments that are completely caught up
	 * 
	 * @throws InterruptedException
	 */
	private void deleteWALSegments() throws IOException {
		if (disableWalDeletion.get()) {
			return;
		}
		write.lock();
		int segmentMarker = Integer.MAX_VALUE;
		String[] array = followerMap.keySet().toArray(new String[0]);
		logger.fine("Follower Count:" + array.length);
		for (String tracker : array) {
			MappedWALFollower follower = followerMap.get(tracker);
			int fId = follower.getFileId() - 1;
			if (fId < segmentMarker) {
				segmentMarker = fId;
			}
		}
		logger.fine("Minimum segment to that can be deleted:" + segmentMarker);
		if (segmentMarker == segmentCounter || segmentMarker == Integer.MAX_VALUE) {
			logger.fine("Minimum segment is also the current segment, ignoring delete");
		} else {
			logger.fine("Segment compaction, will delete:" + segmentMarker + " files");
			for (int i = 1; i < segmentMarker; i++) {
				new File(getSegmentFileName(walDirectory, i)).delete();
				logger.fine("Segment compaction, deleting file:" + getSegmentFileName(walDirectory, i));
			}
		}
		write.unlock();
	}

	private class MappedWALFollower {

		private int fileId;
		private int offset;
		private RandomAccessFile reader;
		private MappedByteBuffer buf;
		private boolean isr;

		/**
		 * @return the reader
		 */
		public RandomAccessFile getReader() {
			return reader;
		}

		/**
		 * @param reader
		 *            the reader to set
		 */
		public void setReader(RandomAccessFile reader) {
			this.reader = reader;
		}

		/**
		 * @return the buf
		 */
		public MappedByteBuffer getBuf() {
			return buf;
		}

		/**
		 * @param buf
		 *            the buf to set
		 */
		public void setBuf(MappedByteBuffer buf) {
			this.buf = buf;
		}

		/**
		 * @return the fileId
		 */
		public int getFileId() {
			return fileId;
		}

		/**
		 * @param fileId
		 *            the fileId to set
		 */
		public void setFileId(int fileId) {
			this.fileId = fileId;
		}

		/**
		 * @return the offset
		 */
		public int getOffset() {
			return offset;
		}

		/**
		 * @param offset
		 *            the offset to set
		 */
		public void setOffset(int offset) {
			this.offset = offset;
		}

		/**
		 * @return the isr
		 */
		public boolean isIsr() {
			return isr;
		}

		/**
		 * @param isr
		 *            the isr to set
		 */
		public void setIsr(boolean isr) {
			this.isr = isr;
		}

	}

	@Override
	public Collection<String> getFollowers() {
		return followerMap.keySet();
	}

	@Override
	public boolean isIsr(String followerId) {
		return followerMap.get(followerId).isr;
	}
}
