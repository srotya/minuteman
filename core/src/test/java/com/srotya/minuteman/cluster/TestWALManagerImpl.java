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
package com.srotya.minuteman.cluster;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Test;

import com.srotya.minuteman.connectors.ClusterConnector;
import com.srotya.minuteman.connectors.ConfigConnector;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc.ReplicationServiceBlockingStub;
import com.srotya.minuteman.utils.FileUtils;
import com.srotya.minuteman.wal.WAL;
import com.srotya.minuteman.rpc.RouteRequest;
import com.srotya.minuteman.rpc.RouteResponse;

import io.grpc.CompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * @author ambud
 */
public class TestWALManagerImpl {

	private static final ScheduledExecutorService es = Executors.newScheduledThreadPool(1);

	@Test
	public void testInit() throws IOException, InterruptedException {
		FileUtils.delete(new File("target/mgrwal1"));
		Map<String, String> conf = new HashMap<>();
		conf.put(WAL.WAL_DIR, "target/mgrwal1");
		conf.put(WAL.WAL_SEGMENT_SIZE, String.valueOf(102400));
		WALManager mgr = new WALManagerImpl();
		try {
			mgr.init(conf, null, es);
			fail("Must fail since connector is null");
		} catch (Exception e) {
		}
		ClusterConnector connector = new ConfigConnector();
		try {
			connector.init(conf);
		} catch (Exception e) {
			fail("Shouldn't throw exception:" + e.getMessage());
		}
		try {
			mgr.init(conf, connector, es);
		} catch (Exception e) {
			fail("Shouldn't throw exception:" + e.getMessage());
		}
		assertNotNull(mgr.getCoordinator());
		try {
			mgr.addRoutableKey("key", 3);
			fail("Should throw insufficient nodes exception");
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		try {
			List<Replica> replica = mgr.addRoutableKey("key", 1);
			assertEquals("localhost:55021", replica.get(0).getLeaderNodeKey());
			assertEquals("localhost:55021", replica.get(0).getReplicaNodeKey());
		} catch (Exception e) {
			fail("Shouldn't throw insufficient nodes exception");
		}
		ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 55021)
				.compressorRegistry(CompressorRegistry.getDefaultInstance()).usePlaintext(true).build();

		ReplicationServiceBlockingStub stub = ReplicationServiceGrpc.newBlockingStub(channel);
		RouteResponse response = stub
				.addRoute(RouteRequest.newBuilder().setRouteKey("test").setReplicationFactor(1).build());
		assertEquals(200, response.getResponseCode());

		try {
			assertNotNull(mgr.getWAL("test"));
		} catch (IOException e) {
			fail("Must not fail to get the WAL");
		}

		mgr.stop();
	}

	@Test
	public void testCluster() throws IOException, InterruptedException {
		FileUtils.delete(new File("target/mgrwal2"));
		ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
		Map<String, String> conf = new HashMap<>();
		conf.put(WAL.WAL_DIR, "target/mgrwal2");
		conf.put(WAL.WAL_SEGMENT_SIZE, String.valueOf(102400));
		WALManagerImpl mgr = new WALManagerImpl();
		ClusterConnector connector = new ConfigConnector();
		conf.put(ConfigConnector.CLUSTER_CC_SLAVES, "localhost:55021,localhost:55022");
		try {
			connector.init(conf);
		} catch (Exception e) {
			fail("Shouldn't throw exception:" + e.getMessage());
		}
		try {
			mgr.init(conf, connector, es);
		} catch (Exception e) {
			fail("Shouldn't throw exception:" + e.getMessage());
		}
		assertNotNull(mgr.getCoordinator());
		try {
			mgr.addRoutableKey("key", 4);
			fail("Should throw insufficient nodes exception");
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		conf.put(WALManager.CLUSTER_GRPC_PORT, "55022");
		ClusterConnector connector2 = new ConfigConnector();
		WALManager mgr2 = new WALManagerImpl();
		try {
			connector2.init(conf);
			mgr2.init(conf, connector2, es);
		} catch (Exception e) {
			fail("Shouldn't throw exception:" + e.getMessage());
		}

		assertTrue(connector.isCoordinator());
		assertTrue(!connector2.isCoordinator());
		try {
			List<Replica> replica = mgr.addRoutableKey("key", 2);
			assertEquals("localhost:55021", replica.get(0).getLeaderNodeKey());
			assertEquals("localhost:55021", replica.get(0).getReplicaNodeKey());
			assertEquals("localhost:55021", replica.get(1).getLeaderNodeKey());
			assertEquals("localhost:55022", replica.get(1).getReplicaNodeKey());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Shouldn't throw insufficient nodes exception");
		}
		ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 55022)
				.compressorRegistry(CompressorRegistry.getDefaultInstance()).usePlaintext(true).build();

		ReplicationServiceBlockingStub stub = ReplicationServiceGrpc.newBlockingStub(channel);
		RouteResponse response = stub
				.addRoute(RouteRequest.newBuilder().setRouteKey("test5").setReplicationFactor(1).build());
		assertEquals(500, response.getResponseCode());

		try {
			assertNull(mgr2.getWAL("test5"));
		} catch (IOException e) {
			fail("Must not fail to get the WAL");
		}

		channel.shutdownNow();

		channel = ManagedChannelBuilder.forAddress("localhost", 55021)
				.compressorRegistry(CompressorRegistry.getDefaultInstance()).usePlaintext(true).build();

		stub = ReplicationServiceGrpc.newBlockingStub(channel);
		response = stub.addRoute(RouteRequest.newBuilder().setRouteKey("test5").setReplicationFactor(2).build());
		assertEquals(200, response.getResponseCode());

		mgr.stop();
	}
}
