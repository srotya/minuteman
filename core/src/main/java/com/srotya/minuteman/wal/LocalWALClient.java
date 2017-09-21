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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author ambud
 */
public class LocalWALClient extends WALClient {

	public static final String MAX_FETCH_BYTES = "max.fetch.bytes";
	private static final Logger logger = Logger.getLogger(LocalWALClient.class.getName());
	private WAL wal;
	private AtomicBoolean ctrl;
	private AtomicInteger counter;
	private String nodeId;
	private int offset;
	private int fileId;
	private int maxFetchBytes;

	public LocalWALClient() {
	}

	public LocalWALClient configure(Map<String, String> conf, String nodeId, WAL localWAL) throws IOException {
		this.wal = localWAL;
		this.maxFetchBytes = Integer.parseInt(conf.getOrDefault(MAX_FETCH_BYTES, String.valueOf(1024 * 1024)));
		this.nodeId = nodeId;
		this.counter = new AtomicInteger(0);
		this.ctrl = new AtomicBoolean(true);
		offset = 4;
		return this;
	}

	@Override
	public void run() {
		try {
			while (ctrl.get()) {
				try {
					WALRead read = wal.read(nodeId, offset, maxFetchBytes, fileId);
					if (read.getData() == null) {
						// System.err.println(
						// "No data:" + nodeId + "\t" + fileId + "\tRead offset:" + read.getNextOffset()
						// + "\t" + wal.getSegmentCounter()
						// + "\t" + wal.getCurrentOffset() + "\t" + wal.getCommitOffset());
						Thread.sleep(100);
					} else {
						processData(read.getData());
						counter.incrementAndGet();
					}
					offset = read.getNextOffset();
					if (read.getFileId() > fileId) {
						offset = 4;
						fileId = read.getFileId();
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failure to replicate WAL", e);
		}
	}

	public int getPos() {
		return wal.getCurrentOffset();
	}

	public int getSegmentCounter() {
		return wal.getSegmentCounter();
	}

	public WAL getWal() {
		return wal;
	}

	public void stop() {
		ctrl.set(false);
	}

	public void processData(byte[] data) {
		// do nothing here
	}
}
