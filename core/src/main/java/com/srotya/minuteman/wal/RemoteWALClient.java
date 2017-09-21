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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.minuteman.rpc.BatchDataRequest;
import com.srotya.minuteman.rpc.BatchDataResponse;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc.ReplicationServiceBlockingStub;

import io.grpc.ManagedChannel;

/**
 * @author ambud
 */
public class RemoteWALClient extends WALClient {

	public static final String MAX_FETCH_BYTES = "max.fetch.bytes";
	private static final Logger logger = Logger.getLogger(RemoteWALClient.class.getName());
	private WAL wal;
	private AtomicBoolean ctrl;
	private AtomicInteger counter;
	private ReplicationServiceBlockingStub stub;
	private String nodeId;
	private int offset;
	private int fileid;
	private int maxFetchBytes;
	private AtomicLong metricRequestTime;
	private AtomicLong metricWriteTime;
	private AtomicLong loopCounter;
	private String routeKey;

	public RemoteWALClient() {
		metricRequestTime = new AtomicLong();
		metricWriteTime = new AtomicLong();
		loopCounter = new AtomicLong();
	}

	public RemoteWALClient configure(Map<String, String> conf, String nodeId, ManagedChannel channel, WAL wal, String routeKey)
			throws IOException {
		this.wal = wal;
		this.routeKey = routeKey;
		this.maxFetchBytes = Integer.parseInt(conf.getOrDefault(MAX_FETCH_BYTES, String.valueOf(1024 * 1024)));
		this.nodeId = nodeId;
		this.counter = new AtomicInteger(0);
		this.ctrl = new AtomicBoolean(true);
		this.stub = ReplicationServiceGrpc.newBlockingStub(channel).withCompression("gzip");
		// Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
		// long l = loopCounter.getAndSet(0);
		// if (l == 0) {
		// l = 1;
		// }
		// System.out.println("Avg RequestTime:" + metricRequestTime.getAndSet(0) / l +
		// "ms\tAvg WriteTime:"
		// + metricWriteTime.getAndSet(0) / l + "ms");
		// }, 0, 1, TimeUnit.SECONDS);
		return this;
	}

	@Override
	public void run() {
		offset = wal.getCurrentOffset();
		fileid = wal.getSegmentCounter();
		while (ctrl.get()) {
			try {
				loopCounter.incrementAndGet();
				logger.fine("CLIENT: Requesting data:" + offset);
				long ts = System.currentTimeMillis();
				BatchDataRequest request = BatchDataRequest.newBuilder().setFileId(fileid).setNodeId(nodeId)
						.setOffset(offset).setMaxBytes(maxFetchBytes).setRouteKey(routeKey).build();
				BatchDataResponse response = stub.requestBatchReplication(request);
				ts = System.currentTimeMillis() - ts;
				metricRequestTime.getAndAdd(ts);
				if (response.getData() == null || response.getData().isEmpty()) {
					logger.fine("CLIENT: No data to replicate, delaying poll, offset:" + offset);
					Thread.sleep(1);
				} else {
					ts = System.currentTimeMillis();
					try {
						wal.write(response.getData().toByteArray(), false);
					} catch (Exception e) {
						logger.log(Level.SEVERE, "Failure to write to local WAL", e);
					}
					ts = System.currentTimeMillis() - ts;
					metricWriteTime.getAndAdd(ts);
					logger.fine("CLIENT: Client received:" + response.getData().size() + " bytes\tfileid:"
							+ response.getFileId());
					counter.addAndGet(response.getData().size());
					wal.setCommitOffset(response.getCommitOffset());
				}
				if (response.getFileId() > fileid) {
					logger.fine("CLIENT: File rotation requested:" + offset + "\t" + fileid);
				}
				offset = response.getNextOffset();
				fileid = response.getFileId();
			} catch (Exception e) {
				logger.log(Level.FINE, "Failure to replicate WAL", e);
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
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
}
