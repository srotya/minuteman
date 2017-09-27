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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
	// TODO potential bug since commit file is not tracked
	private volatile int commitOffset = 0;
	private volatile int commitSegment = 0;
	private AtomicBoolean walDeletion;
	private RandomAccessFile metaraf;
	private MappedByteBuffer metaBuf;

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
		walDeletion = new AtomicBoolean(Boolean.parseBoolean(conf.getOrDefault(WAL_DELETION_DISABLED, "true")));
		logger.info("WAL deletion is set to:" + walDeletion.get());
		// isr polling thread
		es.scheduleAtFixedRate(() -> {
			for (Entry<String, MappedWALFollower> entry : followerMap.entrySet()) {
				MappedWALFollower follower = entry.getValue();
				String followerId = entry.getKey();
				if (segmentCounter != follower.getSegmentId()
						|| currentWrite.position() - follower.getOffset() > isrThreshold) {
					follower.setIsr(false);
					logger.fine("Follower no longer an ISR:" + followerId + " wal:" + walDirectory);
				} else {
					follower.setIsr(true);
					logger.fine("Follower now an ISR:" + followerId + " wal:" + walDirectory);
				}
			}
		}, Integer.parseInt(conf.getOrDefault(WAL_ISRCHECK_DELAY, "1")),
				Integer.parseInt(conf.getOrDefault(WAL_ISRCHECK_FREQUENCY, "10")), TimeUnit.SECONDS);

		// initialize metaraf
		initAndRecoverMetaRaf();
		// initialize the wal segments
		checkAndRotateSegment(0);
	}

	private void initAndRecoverMetaRaf() throws IOException {
		metaraf = new RandomAccessFile(walDirectory + "/.md", "rwd");
		boolean forward = false;
		if (metaraf.length() > 0) {
			forward = true;
		}
		metaBuf = metaraf.getChannel().map(MapMode.READ_WRITE, 0, 1024);
		if (forward) {
			System.out.println("\n\n Found md file with commit offset\n\n");
			commitOffset = metaBuf.getInt();
			commitSegment = metaBuf.getInt();
		} else {
			metaBuf.putInt(0);
			metaBuf.putInt(0);
		}
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
		checkAndRotateSegment(data.length + Integer.BYTES);
		write.lock();
		try {
			counter++;
			currentWrite.putInt(data.length);
			currentWrite.put(data);
			currentWrite.putInt(0, currentWrite.position());
			if (maxCounter != -1 && (flush || counter == maxCounter)) {
				flush();
				counter = 0;
			}
			logger.fine("Wrote data:" + data.length + " at new offset:" + currentWrite.position() + " new file:"
					+ segmentCounter);
		} finally {
			write.unlock();
		}
	}

	@Override
	public WALRead read(String followerId, int offset, int maxBytes, int segmentId, boolean readCommitted)
			throws IOException {
		read.lock();
		MappedWALFollower follower = followerMap.get(followerId);
		if (follower == null) {
			follower = new MappedWALFollower();
			followerMap.put(followerId, follower);
			logger.info("No follower entry, creating one for:" + followerId);
		}
		read.unlock();
		WALRead readData = new WALRead();
		if (segmentId != follower.getSegmentId()) {
			logger.fine("Follower file(" + follower.getSegmentId() + ") is doesn't match requested file id(" + segmentId
					+ "), reseting buffer & file id");
			follower.setBuf(null);
			follower.setSegmentId(segmentId);
			if (follower.getReader() != null) {
				logger.fine("Follower(" + followerId + ") has existing open file, now closing");
				follower.getReader().close();
			}
			deleteWALSegments();
		}
		if (follower.getBuf() == null) {
			File file = new File(getSegmentFileName(walDirectory, segmentId));
			if (!file.exists() && segmentId < segmentCounter) {
				logger.fine("Follower(" + followerId + ") requested file(" + segmentId + "/" + file.getAbsolutePath()
						+ ") doesn't exist, incrementing");
				readData.setSegmentId(segmentId + 1);
				readData.setNextOffset(0);
				return readData;
			}
			logger.fine("Follower(" + followerId + "), opening new wal segment to read:" + file.getAbsolutePath());
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			follower.setReader(raf);
			follower.setBuf(raf.getChannel().map(MapMode.READ_ONLY, 0, file.length()));
			// skip the header
			follower.getBuf().getInt();
		}
		follower.setOffset(offset);
		int pos = 4;
		if (readCommitted) {
			// if (segmentId > commitSegment) {
			// // correct segmentId and let follower know about it
			// // follower should make the correct request in the next request
			// logger.info("\n\nFixing commit segment " + segmentId + " " + commitSegment +
			// "\n\n");
			// readData.setSegmentId(commitSegment);
			// return readData;
			// }
			// pos = commitOffset;
		} else {
			pos = follower.getBuf().getInt(0);
		}
		ByteBuffer buf = follower.getBuf();
		buf.position(offset);
		if (offset < pos) {
			// int length = maxBytes < (pos - offset) ? maxBytes : (pos - offset);
			List<byte[]> data = new ArrayList<>();
			int length = 0;
			int temp = offset;
			do {
				buf.position(temp);
				int currPayloadLength = buf.getInt();
				byte[] dst = new byte[currPayloadLength];
				buf.get(dst);
				data.add(dst);
				length += currPayloadLength + Integer.BYTES;
				temp = offset + length;
			} while (length <= maxBytes && temp < pos);
			// System.out.println("//error:" + offset + " " + length + " " + buf.remaining()
			// + " " + temp);
			readData.setData(data);
		}
		if (pos == buf.position() && segmentCounter > follower.getSegmentId()) {
			int newSegment = follower.getSegmentId() + 1;
			logger.fine("Follower(" + followerId
					+ ") doesn't have any more data to read from this segment, incrementing segment(" + newSegment
					+ "); segmentCounter:" + segmentCounter);
			readData.setSegmentId(newSegment);
			readData.setNextOffset(Integer.BYTES);
		} else {
			readData.setSegmentId(segmentId);
			readData.setNextOffset(buf.position());
		}
		updateMinOffset();
		readData.setCommitOffset(getCommitOffset());
		readData.setCommitSegment(commitSegment);
		return readData;
	}

	private void updateMinOffset() {
		write.lock();
		Entry<Integer, Integer> minimumOffset = getMinimumOffset();
		if (minimumOffset.getValue() != Integer.MAX_VALUE) {
			setCommitSegment(minimumOffset.getKey());
			setCommitOffset(minimumOffset.getValue());
		}
		write.unlock();
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
				int pos = currentWrite.getInt(0);
				if (currentWrite.limit() > getCommitOffset()) {
					currentWrite.position(getCommitOffset());
				} else {
					currentWrite.position(pos);
				}
				logger.info("Found existing WAL:" + getSegmentFileName(walDirectory, segmentCounter)
						+ ", forwarding offset to:" + getCommitOffset());
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
		File[] files = new File(walDirectory).listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".wal");
			}
		});
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
		logger.finer("Updated commit offset:" + commitOffset);
		this.commitOffset = commitOffset;
		metaBuf.putInt(0, commitOffset);
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

	private Entry<Integer, Integer> getMinimumOffset() {
		String[] array = followerMap.keySet().toArray(new String[0]);
		int offsetMarker = Integer.MAX_VALUE;
		int segmentCounter = Integer.MAX_VALUE;
		for (String tracker : array) {
			MappedWALFollower follower = followerMap.get(tracker);
			if (!follower.isIsr()) {
				continue;
			}
			int offset = follower.getOffset();
			if (offset < offsetMarker) {
				offsetMarker = offset;
				segmentCounter = follower.getSegmentId();
			}
		}
		return new AbstractMap.SimpleEntry<Integer, Integer>(segmentCounter, offsetMarker);
	}

	@Override
	public void setWALDeletion(boolean status) {
		walDeletion.set(status);
	}

	/**
	 * Delete WAL segments that are completely caught up
	 * 
	 * @throws InterruptedException
	 */
	private void deleteWALSegments() throws IOException {
		if (!walDeletion.get()) {
			return;
		}
		write.lock();
		int segmentMarker = Integer.MAX_VALUE;
		String[] array = followerMap.keySet().toArray(new String[0]);
		logger.fine("Follower Count:" + array.length);
		for (String tracker : array) {
			MappedWALFollower follower = followerMap.get(tracker);
			int fId = follower.getSegmentId() - 1;
			if (fId < segmentMarker) {
				segmentMarker = fId;
			}
		}
		logger.info("Minimum segment to that can be deleted:" + segmentMarker);
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

	@Override
	public Collection<String> getFollowers() {
		return followerMap.keySet();
	}

	@Override
	public boolean isIsr(String followerId) {
		return followerMap.get(followerId).isr;
	}

	@Override
	public int getCommitSegment() {
		return commitSegment;
	}

	@Override
	public void setCommitSegment(int commitSegment) {
		write.lock();
		this.commitSegment = commitSegment;
		metaBuf.putInt(Integer.BYTES, commitSegment);
		write.unlock();
	}

	private class MappedWALFollower {

		private volatile int segmentId;
		private volatile int offset;
		private RandomAccessFile reader;
		private MappedByteBuffer buf;
		private volatile boolean isr;

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
		public int getSegmentId() {
			return segmentId;
		}

		/**
		 * @param segmentId
		 *            the segmentId to set
		 */
		public void setSegmentId(int segmentId) {
			this.segmentId = segmentId;
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

}
