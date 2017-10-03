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
package com.srotya.minuteman.rocksdb.api;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import org.rocksdb.RocksDB;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.srotya.minuteman.cluster.Node;
import com.srotya.minuteman.cluster.WALManager;
import com.srotya.minuteman.rocksdb.rpc.GetRequest;
import com.srotya.minuteman.rocksdb.rpc.GetResponse;
import com.srotya.minuteman.rocksdb.rpc.KeyValuePair;
import com.srotya.minuteman.rocksdb.rpc.PutRequest;
import com.srotya.minuteman.rocksdb.rpc.RocksServiceGrpc;
import com.srotya.minuteman.rocksdb.rpc.RocksServiceGrpc.RocksServiceBlockingStub;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc.ReplicationServiceBlockingStub;
import com.srotya.minuteman.rpc.RouteRequest;
import com.srotya.minuteman.rpc.RouteResponse;
import com.srotya.minuteman.wal.WAL;

@Path("/")
public class RockApi {

	private int shards;
	private WALManager mgr;
	private RocksDB rocks;

	public RockApi(int shards, WALManager mgr, RocksDB rocks) {
		this.shards = shards;
		this.mgr = mgr;
		this.rocks = rocks;
	}

	@GET
	@Consumes({ MediaType.TEXT_HTML })
	public String get(String keys) {
		Gson gson = new Gson();
		Map<String, List<String>> map = new HashMap<>();
		for (String key : gson.fromJson(keys, String[].class)) {
			String str = String.valueOf(key.hashCode() % shards);
			List<String> req = map.get(str);
			if (req == null) {
				req = new ArrayList<>();
				map.put(str, req);
			}
			req.add(key);
		}

		List<KeyValue> results = new ArrayList<>();
		for (Entry<String, List<String>> entry : map.entrySet()) {
			Node node = getLeaderNode(entry.getKey(), mgr, shards);
			try {
				if (mgr.getThisNodeKey().equalsIgnoreCase(node.getNodeKey())) {
					for (String key : entry.getValue()) {
						byte[] val = rocks.get(key.getBytes());
						if (val != null) {
							results.add(new KeyValue(key, Base64.getEncoder().encodeToString(val)));
						} else {
							System.out.println("Value is null fr key:" + key);
						}
					}
				} else {
					RocksServiceBlockingStub stub = RocksServiceGrpc.newBlockingStub(node.getChannel());
					GetRequest.Builder req = GetRequest.newBuilder();
					for (String key : entry.getValue()) {
						req.addKeys(key);
					}
					GetResponse resp = stub.get(req.build());
					for (KeyValuePair pair : resp.getKvPairsList()) {
						results.add(new KeyValue(pair.getKey(),
								Base64.getEncoder().encodeToString(pair.getValue().toByteArray())));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return new Gson().toJson(results);
	}

	@PUT
	@Consumes({ MediaType.TEXT_HTML })
	public void put(String payload) {
		Gson gson = new Gson();
		Map<String, PutRequest.Builder> map = new HashMap<>();
		for (KeyValue pair : gson.fromJson(payload, KeyValue[].class)) {
			String str = String.valueOf(pair.getKey().hashCode() % shards);
			PutRequest.Builder list = map.get(str);
			if (list == null) {
				list = PutRequest.newBuilder();
				list.setDelete(false);
				map.put(str, list);
			}
			list.addKvPairs(KeyValuePair.newBuilder().setKey(pair.getKey())
					.setValue(ByteString.copyFrom(Base64.getDecoder().decode(pair.getValue()))));
		}

		for (Entry<String, PutRequest.Builder> entry : map.entrySet()) {
			Node node = getLeaderNode(entry.getKey(), mgr, shards);
			try {
				if (mgr.getThisNodeKey().equalsIgnoreCase(node.getNodeKey())) {
					WAL wal = mgr.getWAL(entry.getKey());
					wal.write(entry.getValue().build().toByteArray(), false);
				} else {
					RocksServiceBlockingStub stub = RocksServiceGrpc.newBlockingStub(node.getChannel());
					stub.put(entry.getValue().build());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static Node getLeaderNode(String key, WALManager mgr, int shards) {
		String str = String.valueOf(key.hashCode() % shards);
		String replica = mgr.getReplicaLeader(str);
		if (replica == null) {
			Node coordinator = mgr.getCoordinator();
			ReplicationServiceBlockingStub stub = ReplicationServiceGrpc.newBlockingStub(coordinator.getChannel());
			RouteResponse response = stub
					.addRoute(RouteRequest.newBuilder().setRouteKey(str).setReplicationFactor(1).build());
			String leaderid = response.getLeaderid();
			replica = leaderid;
		}
		Node node = mgr.getNodeMap().get(replica);
		return node;
	}

	public static class KeyValue {

		private String key;
		private String value;

		public KeyValue(String key, String value) {
			super();
			this.key = key;
			this.value = value;
		}

		/**
		 * @return the key
		 */
		public String getKey() {
			return key;
		}

		/**
		 * @param key
		 *            the key to set
		 */
		public void setKey(String key) {
			this.key = key;
		}

		/**
		 * @return the value
		 */
		public String getValue() {
			return value;
		}

		/**
		 * @param value
		 *            the value to set
		 */
		public void setValue(String value) {
			this.value = value;
		}

	}
}
