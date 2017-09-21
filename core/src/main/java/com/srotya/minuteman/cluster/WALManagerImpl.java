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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.minuteman.connectors.ClusterConnector;
import com.srotya.minuteman.rpc.ReplicationServiceImpl;
import com.srotya.minuteman.wal.LocalWALClient;
import com.srotya.minuteman.wal.RemoteWALClient;
import com.srotya.minuteman.wal.WAL;

import io.grpc.DecompressorRegistry;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * @author ambud
 */
public class WALManagerImpl extends WALManager {

	private static final Logger logger = Logger.getLogger(WALManagerImpl.class.getName());
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private WriteLock write = lock.writeLock();
	private Map<String, Replica> localReplicaTable;
	private Map<String, List<Replica>> routeTable;
	private Queue<Node> nodes;
	private ClusterConnector connector;
	private Server server;
	private Class<LocalWALClient> walClientClass;

	public WALManagerImpl() {
		super();
		this.nodes = new LinkedList<>();
		this.localReplicaTable = new HashMap<>();
		this.routeTable = new HashMap<>();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void init(Map<String, String> conf, ClusterConnector connector, ScheduledExecutorService bgTasks)
			throws Exception {
		super.init(conf, connector, bgTasks);
		this.connector = connector;
		connector.initializeRouterHooks(this);
		walClientClass = (Class<LocalWALClient>) Class
				.forName(conf.getOrDefault("wal.class", LocalWALClient.class.getName()));
		server = ServerBuilder.forPort(getPort()).decompressorRegistry(DecompressorRegistry.getDefaultInstance())
				.addService(new ReplicationServiceImpl(this)).build().start();
		logger.info("Listening for GRPC requests on port:" + getPort());
	}

	@Override
	public List<Replica> addRoutableKey(String routingKey, int replicationFactor) throws Exception {
		if (!connector.isCoordinator()) {
			throw new UnsupportedOperationException(
					"This is not a coordinator node, can't perform route modifications");
		}
		if (replicationFactor > nodes.size()) {
			throw new IllegalArgumentException("Fewer nodes(" + nodes.size()
					+ ") in the cluster than requested replication factor(" + replicationFactor + ")");
		}
		write.lock();
		try {
			logger.info("Replication factor:" + replicationFactor + " requested for new key:" + routingKey);
			List<Replica> list = routeTable.get(routingKey);
			if (list == null) {
				// logger.info("Nodes in the cluster:" + nodes);
				list = new ArrayList<>();
				routeTable.put(routingKey, list);
				Replica leader = new Replica();
				Node candidate = getNode();
				nodeToReplica(routingKey, leader, candidate, candidate);
				list.add(0, leader);
				if (isLocal(leader)) {
					replicaUpdated(leader);
				} else {
					connector.updateReplicaRoute(this, leader, false);
				}
				for (int i = 1; i < replicationFactor; i++) {
					Node node = getNode();
					Replica replica = new Replica();
					nodeToReplica(routingKey, replica, candidate, node);
					list.add(i, replica);
					if (isLocal(leader)) {
						replicaUpdated(leader);
					} else {
						connector.updateReplicaRoute(this, replica, false);
					}
				}
				// tell the cluster nodes about this
				connector.updateTable(routeTable);
			} else {
				// routetable for key exists
			}
			return list;
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to update route table in distributed metastore", e);
			throw e;
		} finally {
			write.unlock();
		}
	}

	private Node getNode() {
		Node node = nodes.poll();
		nodes.add(node);
		return node;
	}

	private void nodeToReplica(String routingKey, Replica replica, Node leader, Node follower) {
		replica.setLeaderAddress(leader.getAddress());
		replica.setLeaderNodeKey(leader.getNodeKey());
		replica.setLeaderPort(leader.getPort());
		replica.setRouteKey(routingKey);
		replica.setReplicaAddress(follower.getAddress());
		replica.setReplicaPort(follower.getPort());
		replica.setReplicaNodeKey(follower.getNodeKey());
	}

	@Override
	public void addNode(Node node) throws IOException {
		write.lock();
		logger.info("Adding node(" + node.getNodeKey() + ") to WALManager");
		nodes.add(node);
		getNodeMap().put(node.getNodeKey(), node);
		logger.info("Node(" + node.getNodeKey() + ") added to WALManager");
		write.unlock();
	}

	@Override
	public void removeNode(Node node) throws Exception {
		write.lock();
		try {
			nodes.remove(node);
			// if coordinator then fix the routes of all other followers
			if (connector.isCoordinator()) {
				for (Entry<String, List<Replica>> entry : routeTable.entrySet()) {
					List<Replica> value = entry.getValue();
					Replica leader = value.get(0);
					if (node.getNodeKey().equals(leader.getLeaderNodeKey())) {
						value.remove(0);
						leader = value.get(0);
						if (isLocal(leader)) {
							replicaUpdated(leader);
						} else {
							connector.updateReplicaRoute(this, leader, false);
						}
						for (int i = 0; i < value.size(); i++) {
							Replica replica = value.get(i);
							replica.setLeaderAddress(leader.getLeaderAddress());
							replica.setLeaderPort(leader.getLeaderPort());
							replica.setLeaderNodeKey(leader.getLeaderNodeKey());
							// tell the replica that the leader has changed
							connector.updateReplicaRoute(this, replica, false);
						}
					}
				}
				connector.updateTable(routeTable);
			}
			logger.info("Removing node from WALManager");
			Node node2 = getNodeMap().remove(node.getNodeKey());
			if (node2 != null) {
				for (Iterator<Entry<String, Replica>> iterator = localReplicaTable.entrySet().iterator(); iterator
						.hasNext();) {
					Entry<String, Replica> entry = iterator.next();
					if (node.getNodeKey().equals(entry.getValue().getLeaderNodeKey())) {
						entry.getValue().getClient().stop();
						// TODO delete wal
						iterator.remove();
					}
				}
			}
			logger.info("Node removed from WALManager");
		} finally {
			write.unlock();
		}
	}

	@Override
	public void replicaUpdated(Replica replica) throws IOException {
		write.lock();
		try {
			logger.info("Replica added:" + replica);
			Replica local = localReplicaTable.get(replica.getRouteKey());
			if (local == null) {
				local = replica;
				localReplicaTable.put(replica.getRouteKey(), replica);
				replica.setWal(super.initializeWAL(replica.getRouteKey()));
			} else {
				local.getClient().stop();
			}
			// check if WAL is local; this is true only if this node is the
			// leader
			if (!isLocal(local)) {
				Node node = getNodeMap().get(replica.getLeaderNodeKey());
				local.setClient(new RemoteWALClient().configure(getConf(), getThisNodeKey(), node.getChannel(),
						replica.getWal(), replica.getRouteKey()));
				LocalWALClient client = walClientClass.newInstance();
				local.setLocal(client.configure(getConf(), getThisNodeKey(), replica.getWal()));
				Thread th = new Thread(local.getLocal());
				th.start();
			} else {
				LocalWALClient client = walClientClass.newInstance();
				local.setClient(client.configure(getConf(), getThisNodeKey(), replica.getWal()));
			}
			Thread th = new Thread(local.getClient());
			th.start();
			logger.info("Starting replication thread for:" + replica.getRouteKey());
		} catch (InstantiationException | IllegalAccessException e) {
			throw new IOException(e);
		} finally {
			write.unlock();
		}
	}

	private boolean isLocal(Replica replica) {
		return replica.getLeaderNodeKey().equals(this.getThisNodeKey());
	}

	@Override
	public void replicaRemoved(Replica replica) throws Exception {
		write.lock();
		logger.info("Removing replica " + replica.getRouteKey() + " cleaning up entries on " + this.getThisNodeKey());
		Replica replica2 = localReplicaTable.get(replica.getRouteKey());
		if (replica2 != null) {
			replica2.getClient().stop();
			logger.info("Replica removed " + replica.getRouteKey() + " on " + this.getThisNodeKey());
		} else {
			logger.info("Replica already removed, nothing to do");
		}
		write.unlock();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void makeCoordinator() throws Exception {
		write.lock();
		routeTable = (Map<String, List<Replica>>) connector.fetchRoutingTable();
		logger.info("Fetched latest route table from metastore:" + routeTable);
		if (routeTable == null) {
			connector.updateTable(this.routeTable = new HashMap<>());
			logger.info("No route table in metastore, created an empty one");
		} else {
			if (!getCoordinator().equals(connector.getLocalNode())) {
				removeNode(getCoordinator());
			} else {
				logger.warning("Ignoring route table correction because of self last coordinator");
			}
		}
		write.unlock();
	}

	@Override
	public Object getRoutingTable() {
		return localReplicaTable;
	}

	@Override
	public void stop() throws InterruptedException {
		server.shutdown().awaitTermination(100, TimeUnit.SECONDS);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void resume() throws IOException {
		Map<String, List<Replica>> rt = (Map<String, List<Replica>>) connector.fetchRoutingTable();
		if (rt != null) {
			for (Entry<String, List<Replica>> entry : rt.entrySet()) {
				for (Replica replica : entry.getValue()) {
					if (replica.getReplicaNodeKey().equals(getThisNodeKey())) {
						// resume this replica
						logger.info("Found replica assignment for local node, resuming:" + replica.getReplicaNodeKey()
								+ "\t" + replica.getRouteKey());
						replicaUpdated(replica);
					}
				}
			}
		}
	}

	@Override
	public WAL getWAL(String key) throws IOException {
		Replica replica = localReplicaTable.get(key);
		if (replica == null) {
			return null;
		}
		return replica.getWal();
	}

}
