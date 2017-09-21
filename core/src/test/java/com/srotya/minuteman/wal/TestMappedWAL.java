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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Test;

import com.srotya.minuteman.utils.FileUtils;

public class TestMappedWAL {

	private ScheduledExecutorService es = Executors.newScheduledThreadPool(1);

	@Test
	public void testWALConfiguration() throws IOException {
		String walDir = "target/wal1";
		FileUtils.delete(new File(walDir));
		WAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(1024 * 1024 * 2));
		wal.configure(conf, es);
		File[] listFiles = new File(walDir).listFiles();
		assertEquals(1, listFiles.length);
		assertEquals(MappedWAL.getSegmentFileName(walDir, 1), listFiles[0].getPath().replace("\\", "/"));
		assertEquals(1024 * 1024 * 2, listFiles[0].length());
	}

	@Test
	public void testWALWrites() throws IOException {
		String walDir = "target/wal2";
		FileUtils.delete(new File(walDir));
		WAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(1024 * 1024 * 2));
		wal.configure(conf, es);
		for (int i = 0; i < 1000; i++) {
			String str = ("test" + String.format("%03d", i));
			wal.write(str.getBytes(), false);
		}
		wal.flush();
		int expectedBytes = 7 * 1000 + 4;
		RandomAccessFile raf = new RandomAccessFile(MappedWAL.getSegmentFileName(walDir, 1), "r");
		MappedByteBuffer map = raf.getChannel().map(MapMode.READ_ONLY, 0, expectedBytes);
		raf.close();
		map.getInt();
		for (int i = 0; i < 1000; i++) {
			byte[] dst = new byte[7];
			try {
				map.get(dst);
			} catch (Exception e) {
				System.out.println("Marker:" + i);
				throw e;
			}
			assertEquals("test" + String.format("%03d", i), new String(dst));
		}
	}

	@Test
	public void testWALReads() throws IOException {
		String walDir = "target/wal3";
		FileUtils.delete(new File(walDir));
		WAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(1024 * 1024 * 2));
		wal.configure(conf, es);
		for (int i = 0; i < 1000; i++) {
			String str = ("test" + String.format("%03d", i));
			wal.write(str.getBytes(), false);
		}
		WALRead read = wal.read("local", 0, 10000, 1);
		ByteBuffer buf = ByteBuffer.wrap(read.getData());
		buf.getInt();
		for (int i = 0; i < 1000; i++) {
			byte[] dst = new byte[7];
			try {
				buf.get(dst);
			} catch (Exception e) {
				fail("Shouldn't throw exception:" + e.getMessage());
				throw e;
			}
			assertEquals("test" + String.format("%03d", i), new String(dst));
		}
	}

	@Test
	public void testFollowers() throws IOException, InterruptedException {
		String walDir = "target/wal4";
		FileUtils.delete(new File(walDir));
		WAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(1024 * 1024 * 2));
		conf.put(WAL.WAL_ISRCHECK_FREQUENCY, "1");
		conf.put(WAL.WAL_ISR_THRESHOLD, "1024");
		conf.put(WAL.WAL_SEGMENT_FLUSH_COUNT, "100");
		assertEquals(-1, wal.getOffset());
		wal.configure(conf, es);
		int total = 4;
		for (int i = 0; i < 1000; i++) {
			String str = ("test" + String.format("%03d", i));
			wal.write(str.getBytes(), false);
			total += str.length();
		}
		wal.flush();
		assertEquals(total, wal.getOffset());
		// let ISR check thread mark this follower as not ISR
		WALRead read = wal.read("f1", 0, 1024 * 1024, 1);
		assertEquals(total, read.getData().length);
		assertEquals(0, read.getCommitOffset());
		assertEquals(0, wal.getFollowerOffset("f1"));
		total = 0;
		for (int i = 0; i < 1000; i++) {
			String str = ("test" + String.format("%03d", i));
			wal.write(str.getBytes(), false);
			total += str.length();
		}
		wal.flush();
		Thread.sleep(1000);
		assertEquals(false, wal.isIsr("f1"));
		read = wal.read("f1", read.getNextOffset(), 1024 * 1024, 1);
		assertEquals(total, read.getData().length);
		assertEquals(total + 4, wal.getFollowerOffset("f1"));
		total *= 2;
		read = wal.read("f1", read.getNextOffset(), 1024 * 1024, 1);
		// let ISR check thread run and mark this follower as ISR
		Thread.sleep(1000);
		assertTrue(read.getData() == null);
		assertEquals(total + 4, wal.getFollowerOffset("f1"));
		read = wal.read("f1", read.getNextOffset(), 1024 * 1024, 1);
		assertTrue(read.getData() == null);
		assertEquals(total + 4, wal.getFollowerOffset("f1"));
		assertEquals(total + 4, read.getCommitOffset());

		for (int i = 0; i < 1000; i++) {
			String str = ("test" + String.format("%03d", i));
			wal.write(str.getBytes(), false);
		}
		read = wal.read("f1", read.getNextOffset(), 1024 * 1024, 1);
		assertEquals(total + 4, read.getCommitOffset());
	}

	@Test
	public void testWALSegmentRotations() throws IOException {
		String walDir = "target/wal5";
		FileUtils.delete(new File(walDir));
		WAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(5000));
		wal.configure(conf, es);
		for (int i = 0; i < 2000; i++) {
			String str = ("test" + String.format("%03d", i));
			wal.write(str.getBytes(), false);
		}
		assertEquals(4, wal.getSegmentCounter());
		WALRead read = wal.read("local", 0, 10000, 1);
		ByteBuffer buf = ByteBuffer.wrap(read.getData());
		buf.getInt();
		for (int i = 0; i < 713; i++) {
			byte[] dst = new byte[7];
			try {
				buf.get(dst);
			} catch (Exception e) {
				fail("Shouldn't throw exception:" + e.getMessage());
				throw e;
			}
			assertEquals("test" + String.format("%03d", i), new String(dst));
		}
		assertEquals(-1, wal.getFollowerOffset("f1"));
		assertEquals(2, read.getFileId());
		assertEquals(4, read.getNextOffset());
		read = wal.read("local", read.getNextOffset(), 10000, read.getFileId());
		buf = ByteBuffer.wrap(read.getData());
		assertEquals(4993, read.getData().length);
		for (int i = 0; i < 287; i++) {
			byte[] dst = new byte[7];
			try {
				buf.get(dst);
			} catch (Exception e) {
				fail("Shouldn't throw exception:" + e.getMessage());
				throw e;
			}
			assertEquals("test" + String.format("%03d", i + 713), new String(dst));
		}
		read = wal.read("local", 4, 10000, read.getFileId());
		read = wal.read("local", read.getNextOffset(), 10000, read.getFileId());
		assertEquals(4, read.getFileId());
		read = wal.read("local", 4, 10000, 0);
		assertEquals(1, read.getFileId());
	}

	@Test
	public void testWALSegmentRecovery() throws IOException {
		ScheduledExecutorService es1 = Executors.newScheduledThreadPool(1);
		String walDir = "target/wal6";
		FileUtils.delete(new File(walDir));
		WAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(5000));
		wal.configure(conf, es1);
		for (int i = 0; i < 2000; i++) {
			String str = ("test" + String.format("%03d", i));
			wal.write(str.getBytes(), false);
		}
		wal.read("f2", 4, 100, 1);
		wal.close();
		es1.shutdownNow();
		es1 = Executors.newScheduledThreadPool(1);
		wal = new MappedWAL();
		wal.configure(conf, es1);
		assertEquals(4, wal.getSegmentCounter());
		wal.read("f2", 4, 100, 1);
		assertEquals(1, wal.getFollowers().size());
	}
}
