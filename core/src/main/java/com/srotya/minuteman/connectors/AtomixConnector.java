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
package com.srotya.minuteman.connectors;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.minuteman.cluster.Node;
import com.srotya.minuteman.cluster.WALManager;

import io.atomix.AtomixReplica;
import io.atomix.AtomixReplica.Type;
import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.TypeSerializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.Storage.Builder;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.group.DistributedGroup;
import io.atomix.group.GroupMember;
import io.atomix.group.election.Term;
import io.atomix.variables.DistributedValue;

/**
 * @author ambud
 */
public class AtomixConnector extends ClusterConnector {

	private static final String TABLE = "table";
	private static final String BROADCAST_GROUP = "controller";
	private static final Logger logger = Logger.getLogger(AtomixConnector.class.getName());
	private AtomixReplica atomix;
	private boolean isBootstrap;
	private String address;
	private int port;
	private volatile boolean leader;
	private Node localNode;
	private DistributedGroup group;
	protected Node coordinator;

	@Override
	public void init(Map<String, String> conf) throws Exception {
		AtomixReplica.Builder builder = AtomixReplica
				.builder(new Address(conf.getOrDefault("cluster.atomix.host", "localhost"),
						Integer.parseInt(conf.getOrDefault("cluster.atomix.port", "8901"))));
		Builder storageBuilder = Storage.builder();

		storageBuilder
				.withStorageLevel(StorageLevel.valueOf(conf.getOrDefault("cluster.atomix.storage.level", "MEMORY")));
		storageBuilder.withDirectory(conf.getOrDefault("cluster.atomix.storage.directory", "/tmp/sidewinder-atomix"));

		atomix = builder.withStorage(storageBuilder.build()).withSessionTimeout(Duration.ofSeconds(10))
				.withGlobalSuspendTimeout(Duration.ofMinutes(2)).withType(Type.ACTIVE)
				.withElectionTimeout(Duration
						.ofSeconds(Integer.parseInt(conf.getOrDefault("cluster.atomix.election.timeout", "10"))))
				.withHeartbeatInterval(Duration
						.ofSeconds(Integer.parseInt(conf.getOrDefault("cluster.atomix.heartbeat.interval", "5"))))
				.build();

		atomix.serializer().register(Node.class, NodeSerializer.class);

		this.isBootstrap = Boolean.parseBoolean(conf.getOrDefault("cluster.atomix.bootstrap", "true"));
		if (isBootstrap) {
			logger.info("Joining cluster as bootstrap node");
			atomix.bootstrap(new Address(conf.getOrDefault("cluster.atomix.bootstrap.host", "localhost"),
					Integer.parseInt(conf.getOrDefault("cluster.atomix.bootstrap.port", "8901")))).join();
			atomix.getValue(TABLE);
		} else {
			logger.info("Joining cluster as a member node");
			atomix.join(new Address(conf.getOrDefault("cluster.atomix.bootstrap.host", "localhost"),
					Integer.parseInt(conf.getOrDefault("cluster.atomix.bootstrap.port", "8901")))).get();
		}
		logger.info("Atomix clustering initialized");
	}

	@Override
	public void initializeRouterHooks(final WALManager manager) throws IOException {
		port = manager.getPort();
		address = manager.getAddress();
		DistributedGroup.Config config = new DistributedGroup.Config().withMemberExpiration(Duration.ofSeconds(20));
		group = getAtomix().getGroup(BROADCAST_GROUP, config).join();
		group.election().onElection(new Consumer<Term>() {

			@Override
			public void accept(Term t) {
				if (isLocal(t.leader().id())) {
					logger.info("Completed leader election:" + t.leader().id());
					leader = true;
					try {
						manager.makeCoordinator();
					} catch (Exception e) {
						e.printStackTrace();
						logger.severe("Error making corrdinator");
					}
				} else {
					logger.info("Leader election completed, " + t.leader().id() + " is the leader");
					leader = false;
				}
				Node node = manager.getNodeMap().get(t.leader().id());
				if (node == null) {
					logger.info("Leader node is empty:" + t.leader().id());
					node = buildNode(t.leader().id());
				}
				manager.setCoordinator(node);
				coordinator = node;
			}
		});

		group.onJoin(new Consumer<GroupMember>() {

			@Override
			public void accept(GroupMember t) {
				logger.info("Node found:" + t.id());
				Node node = buildNode(t.id());
				try {
					manager.addNode(node);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				manager.getNodeMap().put(t.id(), node);
			}

		});

		group.onLeave(new Consumer<GroupMember>() {

			@Override
			public void accept(GroupMember t) {
				logger.info("Node left:" + t.id());
				Node node = buildNode(t.id());
				try {
					manager.removeNode(node);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				manager.getNodeMap().remove(t.id());
			}
		});

		group.join(address + ":" + port).join();
		logger.info("Created cluster using Atomix connector");

		for (GroupMember groupMember : group.members()) {
			if (isLocal(groupMember.id())) {
				continue;
			}
			Node node = buildNode(groupMember.id());
			manager.addNode(node);
		}

		manager.resume();

		try {
			getAtomix().getValue(TABLE).get().onChange(event -> {
				logger.info("Route table updated by leader:" + event.newValue());
			});
		} catch (InterruptedException | ExecutionException e) {
			logger.log(Level.SEVERE, "Error updating route table on node " + address + ":" + port, e);
		}
	}

	@Override
	public boolean isCoordinator() {
		return leader;
	}

	public AtomixReplica getAtomix() {
		return atomix;
	}

	public boolean isBootstrap() {
		return isBootstrap;
	}

	private boolean isLocal(String id) {
		String[] split = id.split(":");
		return split[0].equalsIgnoreCase(address) && Integer.parseInt(split[1]) == port;
	}

	@Override
	public int getClusterSize() throws Exception {
		return getAtomix().getGroup(BROADCAST_GROUP).join().members().size();
	}

	@Override
	public Object fetchRoutingTable() {
		try {
			logger.info("Fetching route table info from metastore");
			DistributedValue<Object> value = getAtomix().getValue(TABLE).get(2, TimeUnit.SECONDS);
			logger.info("Fetched route table info from metastore:" + value.get());
			return value.get().get();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to fetch routing table", e);
		}
		return null;
	}

	@Override
	public void updateTable(Object table) throws Exception {
		getAtomix().getValue(TABLE).get().set(table);
		logger.info("Updated route table in atomix");
	}

	@Override
	public Node getLocalNode() {
		return localNode;
	}

	@SuppressWarnings("rawtypes")
	public static class NodeSerializer implements TypeSerializer<Node> {

		@Override
		public Node read(Class<Node> arg0, BufferInput arg1, Serializer arg2) {
			String address = arg1.readUTF8();
			int port = arg1.readInt();
			return new Node(address + ":" + port, address, port);
		}

		@Override
		public void write(Node node, BufferOutput arg1, Serializer arg2) {
			arg1.writeUTF8(node.getAddress());
			arg1.writeInt(node.getPort());
		}

	}

	@Override
	public Node getCoordinator() {
		return coordinator;
	}

}
