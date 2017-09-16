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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
public class WAL {

	private static final Logger logger = Logger.getLogger(WAL.class.getName());
	private ReadLock read;
	private WriteLock write;
	private MappedByteBuffer currentWrite;
	private int segmentSize;
	private int segmentCounter;
	private RandomAccessFile raf;
	private String walDirectory;
	private Map<String, Follower> followerMap;
	private int counter;
	private int maxCounter;

	public WAL() {
	}

	public void configure(Map<String, String> conf) throws IOException, InterruptedException {
		walDirectory = conf.getOrDefault("wal.dir", "target/wal");
		new File(walDirectory).mkdirs();
		// maximum size of a segment
		// each segment is memory mapped in whole; default's 100MB
		segmentSize = Integer.parseInt(conf.getOrDefault("wal.segment.size", String.valueOf(1024 * 1024 * 200)));
		// maximum number of writes after which a force flush is triggered
		maxCounter = Integer.parseInt(conf.getOrDefault("wal.segment.flush.count", String.valueOf(-1)));
		// initialize read/write lock
		ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		read = lock.readLock();
		write = lock.writeLock();
		// create an empty map of all followers
		followerMap = new ConcurrentHashMap<>();
		// initialize the wal segments
		checkAndRotateSegment(0);
	}

	public MappedByteBuffer getCurrentWriteBuffer() {
		return currentWrite;
	}

	public void write(byte[] data, boolean flush) throws IOException {
		checkAndRotateSegment(data.length);
		write.lock();
		try {
			counter++;
			currentWrite.put(data);
			currentWrite.putInt(0, currentWrite.position());
			if (maxCounter != -1 && (flush || counter == maxCounter)) {
				currentWrite.force();
				counter = 0;
			}
		} finally {
			write.unlock();
		}
	}

	public WALRead read(String followerId, int offset, int maxBytes, int fileId) throws IOException {
		read.lock();
		Follower follower = followerMap.get(followerId);
		if (follower == null) {
			follower = new Follower();
			followerMap.put(followerId, follower);
			logger.info("No follower entry, creating one for:" + followerId);
		}
		read.unlock();
		WALRead readData = new WALRead();
		if (fileId != follower.getFileId()) {
			logger.info("Follower file(" + follower.getFileId() + ") is doesn't match requested file id(" + fileId
					+ "), reseting buffer & file id");
			follower.setBuf(null);
			follower.setFileId(fileId);
		}
		if (follower.getBuf() == null) {
			if (follower.getReader() != null) {
				logger.info("Follower(" + followerId + ") has existing open file, now closing");
				follower.getReader().close();
			}
			File file = new File(getSegmentFileName(walDirectory, fileId));
			if (!file.exists() && fileId < segmentCounter) {
				logger.info("Follower(" + followerId + ") requested file(" + fileId + ") doesn't exist, incrementing");
				readData.setFileId(fileId + 1);
				readData.setNextOffset(0);
				return readData;
			}
			logger.info("Follower(" + followerId + "), opening new wal segment to read:" + file.getAbsolutePath());
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			follower.setReader(raf);
			follower.setBuf(raf.getChannel().map(MapMode.READ_ONLY, 0, file.length()));
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
		} else if (segmentCounter > follower.getFileId()) {
			int newSegment = follower.getFileId() + 1;
			logger.info("Follower(" + followerId
					+ ") doesn't have any more data to read from this segment, incrementing segment(" + newSegment
					+ "); segmentCounter:" + segmentCounter);
			readData.setFileId(newSegment);
		} else {
			readData.setFileId(fileId);
		}
		readData.setNextOffset(buf.position());
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
				deleteWALSegments();
			}
			boolean forward = false;
			if (currentWrite == null) {
				segmentCounter = getLastSegmentFileName(walDirectory);
				if (segmentCounter > 1) {
					forward = true;
				}
			} else {
				segmentCounter++;
			}
			raf = new RandomAccessFile(new File(getSegmentFileName(walDirectory, segmentCounter)), "rwd");
			currentWrite = raf.getChannel().map(MapMode.READ_WRITE, 0, segmentSize);
			if (forward) {
				// if this segment file already exists then forward the cursor to the
				// latest committed location;
				int offset = currentWrite.getInt(0);
				currentWrite.position(offset);
				logger.info("Found existing WAL:" + getSegmentFileName(walDirectory, segmentCounter)
						+ ", forwarding offset to:" + offset);
			} else {
				// else write a blank 0 offset to the beginning of this brand new buffer
				currentWrite.putInt(0, 0);
			}
			logger.info("Rotating segment file:" + getSegmentFileName(walDirectory, segmentCounter));
			write.unlock();
		}
	}

	private static String getSegmentFileName(String walDirectory, int segmentCounter) {
		return walDirectory + "/" + String.format("%012d", segmentCounter) + ".wal";
	}

	private static int getLastSegmentFileName(String walDirectory) {
		File[] files = new File(walDirectory).listFiles();
		if (files.length > 0) {
			return Integer.parseInt(files[files.length - 1].getName().replace(".wal", ""));
		} else {
			return 1;
		}
	}

	/**
	 * Delete WAL segments that are completely caught up
	 * 
	 * @throws InterruptedException
	 */
	private void deleteWALSegments() throws IOException {
		int segmentMarker = Integer.MAX_VALUE;
		String[] array = followerMap.keySet().toArray(new String[0]);
		logger.info("Follower Count:" + array.length);
		for (String tracker : array) {
			Follower follower = followerMap.get(tracker);
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
				logger.info("Segment compaction, deleting file:" + getSegmentFileName(walDirectory, i));
			}
		}
	}
}
