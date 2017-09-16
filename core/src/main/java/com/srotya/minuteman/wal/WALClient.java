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

import com.srotya.minuteman.rpc.BatchDataRequest;
import com.srotya.minuteman.rpc.BatchDataResponse;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc.ReplicationServiceBlockingStub;

import io.grpc.ManagedChannel;

/**
 * @author ambud
 */
public class WALClient implements Runnable {

	private static final Logger logger = Logger.getLogger(WALClient.class.getName());
	private WAL wal;
	private AtomicBoolean ctrl;
	private AtomicInteger counter;
	private ReplicationServiceBlockingStub stub;
	private String nodeId;
	private int offset;
	private int fileid;
	private int maxFetchBytes;

	public void configure(Map<String, String> conf, String nodeId, ManagedChannel channel)
			throws IOException, InterruptedException {
		this.maxFetchBytes = Integer.parseInt(conf.getOrDefault("max.fetch.bytes", String.valueOf(1024 * 1024 * 2)));
		this.nodeId = nodeId;
		this.counter = new AtomicInteger(0);
		this.wal = new WAL();
		this.wal.configure(conf);
		this.ctrl = new AtomicBoolean(true);
		this.stub = ReplicationServiceGrpc.newBlockingStub(channel);
	}

	@Override
	public void run() {
		try {
			offset = 0;
			fileid = 1;
			long id = 0;
			while (ctrl.get()) {
				logger.fine("CLIENT: Requesting data:" + offset);
				BatchDataRequest request = BatchDataRequest.newBuilder().setMessageId(id).setFileId(fileid)
						.setNodeId(nodeId).setOffset(offset).setMaxBytes(maxFetchBytes).build();
				BatchDataResponse response = stub.requestBatchReplication(request);
				id = response.getMessageId();
				if (response.getData() == null || response.getData().isEmpty()) {
					logger.info("CLIENT: No data to replicate, delaying poll, offset:" + offset);
					Thread.sleep(100);
				} else {
					try {
						wal.write(response.getData().toByteArray(), false);
					} catch (Exception e) {
						logger.log(Level.SEVERE, "Failure to write to local WAL", e);
					}
					logger.fine("CLIENT: Client received:" + response.getData().size() + " bytes\tfileid:"
							+ response.getFileId());
					offset = response.getNextOffset();
					counter.addAndGet(response.getData().size());
				}
				if (response.getFileId() > fileid) {
					logger.info("CLIENT: File rotation requested:" + offset + "\t" + fileid);
					offset = 0;
					fileid = response.getFileId();
				}
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failure to replicate WAL", e);
		}
	}

	public int getPos() {
		return wal.getCurrentWriteBuffer().position();
	}

}
