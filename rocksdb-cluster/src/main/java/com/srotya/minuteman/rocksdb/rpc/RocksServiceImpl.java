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
package com.srotya.minuteman.rocksdb.rpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.rocksdb.RocksDB;

import com.google.protobuf.ByteString;
import com.srotya.minuteman.cluster.Node;
import com.srotya.minuteman.cluster.WALManager;
import com.srotya.minuteman.rocksdb.rpc.RocksServiceGrpc.RocksServiceBlockingStub;
import com.srotya.minuteman.rocksdb.rpc.RocksServiceGrpc.RocksServiceImplBase;
import com.srotya.minuteman.wal.WAL;

import io.grpc.stub.StreamObserver;

/**
 * @author ambud
 */
public class RocksServiceImpl extends RocksServiceImplBase {

	private WALManager mgr;
	private RocksDB rocks;
	private int shards;

	public RocksServiceImpl(int shards, WALManager mgr, RocksDB rocks) {
		this.shards = shards;
		this.mgr = mgr;
		this.rocks = rocks;
	}

	@Override
	public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
		if (request.getKeysList() == null) {
			// return invalid
		}
		Map<String, List<String>> map = new HashMap<>();
		for (String key : request.getKeysList()) {
			String str = String.valueOf(key.hashCode() % shards);
			List<String> req = map.get(str);
			if (req == null) {
				req = new ArrayList<>();
				map.put(str, req);
			}
			req.add(key);
		}

		GetResponse.Builder builder = GetResponse.newBuilder();
		for (Entry<String, List<String>> entry : map.entrySet()) {
			Node node = getLeaderNode(entry.getKey(), mgr, shards);
			try {
				if (mgr.getThisNodeKey().equalsIgnoreCase(node.getNodeKey())) {
					for (String key : entry.getValue()) {
						byte[] val = rocks.get(key.getBytes());
						builder.addKvPairs(KeyValuePair.newBuilder().setKey(key).setValue(ByteString.copyFrom(val)));
					}
				} else {
					RocksServiceBlockingStub stub = RocksServiceGrpc.newBlockingStub(node.getChannel());
					GetRequest.Builder req = GetRequest.newBuilder();
					for (String key : entry.getValue()) {
						req.addKeys(key);
					}
					GetResponse response = stub.get(req.build());
					builder.addAllKvPairs(response.getKvPairsList());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		responseObserver.onNext(builder.build());
		responseObserver.onCompleted();
	}

	@Override
	public void delete(DeleteRequest request, StreamObserver<GenericResponse> responseObserver) {
		if (request.getKeysList() == null) {
			// return invalid
		} else {
			Map<String, DeleteRequest.Builder> map = new HashMap<>();
			for (String key : request.getKeysList()) {
				String str = String.valueOf(key.hashCode() % shards);
				DeleteRequest.Builder req = map.get(str);
				if (req == null) {
					req = DeleteRequest.newBuilder();
					map.put(str, req);
				}
				req.addKeys(key);
			}
			for (Entry<String, DeleteRequest.Builder> entry : map.entrySet()) {
				Node node = getLeaderNode(entry.getKey(), mgr, shards);
				try {
					GenericResponse delete = null;
					if (mgr.getThisNodeKey().equalsIgnoreCase(node.getNodeKey())) {
						String str = String.valueOf(entry.getKey().hashCode() % shards);
						WAL wal = mgr.getWAL(str);
						wal.write(entry.getValue().build().toByteArray(), false);
						delete = GenericResponse.newBuilder().setResponseCode(200).build();
					} else {
						RocksServiceBlockingStub stub = RocksServiceGrpc.newBlockingStub(node.getChannel());
						delete = stub.delete(entry.getValue().build());
					}
					responseObserver.onNext(delete);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		responseObserver.onCompleted();
	}

	public static Node getLeaderNode(String str, WALManager mgr, int shards) {
		String replica = mgr.getReplicaLeader(str);
		Node node = mgr.getNodeMap().get(replica);
		return node;
	}

	@Override
	public void put(PutRequest request, StreamObserver<GenericResponse> responseObserver) {
		if (request.getKvPairsList() == null) {
			// return invalid
		} else {
			Map<String, PutRequest.Builder> map = new HashMap<>();
			for (KeyValuePair pair : request.getKvPairsList()) {
				String str = String.valueOf(pair.getKey().hashCode() % shards);
				PutRequest.Builder list = map.get(str);
				if (list == null) {
					list = PutRequest.newBuilder();
					map.put(str, list);
				}
				list.addKvPairs(pair);
			}

			for (Entry<String, PutRequest.Builder> entry : map.entrySet()) {
				Node node = getLeaderNode(entry.getKey(), mgr, shards);
				try {
					if (mgr.getThisNodeKey().equalsIgnoreCase(node.getNodeKey())) {
						WAL wal = mgr.getWAL(entry.getKey());
						wal.write(entry.getValue().build().toByteArray(), false);
					} else {
						RocksServiceBlockingStub stub = RocksServiceGrpc.newBlockingStub(node.getChannel());
						GenericResponse response = stub.put(entry.getValue().build());
						responseObserver.onNext(response);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		responseObserver.onCompleted();
	}

}
