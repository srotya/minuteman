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
package com.srotya.minutema.rpc;

import java.io.IOException;

import com.google.protobuf.ByteString;
import com.srotya.minuteman.rpc.BatchDataRequest;
import com.srotya.minuteman.rpc.BatchDataResponse;
import com.srotya.minuteman.rpc.BatchDataResponse.Builder;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc.ReplicationServiceImplBase;
import com.srotya.minuteman.wal.WAL;
import com.srotya.minuteman.wal.WALRead;

import io.grpc.stub.StreamObserver;

/**
 * @author ambud
 */
public class ReplicationServiceImpl extends ReplicationServiceImplBase {

	private WAL wal;

	public ReplicationServiceImpl(WAL wal) {
		this.wal = wal;
	}

	@Override
	public void requestBatchReplication(BatchDataRequest request, StreamObserver<BatchDataResponse> responseObserver) {
		try {
			WALRead read = wal.read(request.getNodeId(), request.getOffset(), request.getMaxBytes(),
					request.getFileId());
			Builder builder = BatchDataResponse.newBuilder().setMessageId(System.nanoTime())
					.setNextOffset(read.getNextOffset());
			if (read.getData() != null) {
				builder.setData(ByteString.copyFrom(read.getData()));
			}
			builder.setFileId(read.getFileId());
			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
