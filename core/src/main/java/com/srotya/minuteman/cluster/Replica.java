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

import com.srotya.minuteman.wal.WAL;
import com.srotya.minuteman.wal.WALClient;

/**
 * A node is a logical member of the Sidewinder cluster where one more more
 * nodes form a cluster.
 * 
 * The reason node is referred to as a logical member is because it may
 * represent a physical node or a virtual node that is created to for the
 * purpose of even distribution of data.
 * 
 * The {@link Replica} object simply provides a mechanism for the
 * {@link RoutingStrategy} to compute placement location and provides a helper
 * {@link EndpointService} concept to avoid using yet another lookup table.
 * 
 * @author ambud
 */
public class Replica {

	private String routeKey;
	private String replicaAddress;
	private String replicaNodeKey;
	private int replicaPort;
	private String leaderNodeKey;
	private String leaderAddress;
	private int leaderPort;
	private transient WAL wal;
	private transient WALClient client;
	private transient WALClient local;
	
	public Replica() {
	}

	public Replica(String replicaNodeKey) {
		this.replicaNodeKey = replicaNodeKey;
	}

	/**
	 * @return the replicaAddress
	 */
	public String getReplicaAddress() {
		return replicaAddress;
	}

	/**
	 * @param replicaAddress the replicaAddress to set
	 */
	public void setReplicaAddress(String replicaAddress) {
		this.replicaAddress = replicaAddress;
	}

	/**
	 * @return the replicaNodeKey
	 */
	public String getReplicaNodeKey() {
		return replicaNodeKey;
	}

	/**
	 * @param replicaNodeKey the replicaNodeKey to set
	 */
	public void setReplicaNodeKey(String replicaNodeKey) {
		this.replicaNodeKey = replicaNodeKey;
	}

	/**
	 * @return the replicaPort
	 */
	public int getReplicaPort() {
		return replicaPort;
	}

	/**
	 * @param replicaPort the replicaPort to set
	 */
	public void setReplicaPort(int replicaPort) {
		this.replicaPort = replicaPort;
	}

	/**
	 * @return the leaderNodeKey
	 */
	public String getLeaderNodeKey() {
		return leaderNodeKey;
	}

	/**
	 * @param leaderNodeKey
	 *            the leaderNodeKey to set
	 */
	public void setLeaderNodeKey(String leaderNodeKey) {
		this.leaderNodeKey = leaderNodeKey;
	}

	/**
	 * @return the routeKey
	 */
	public String getRouteKey() {
		return routeKey;
	}

	/**
	 * @param routeKey
	 *            the routeKey to set
	 */
	public void setRouteKey(String routeKey) {
		this.routeKey = routeKey;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Replica)) {
			return false;
		} else {
			return false;
		}
	}

	/**
	 * @return the leaderAddress
	 */
	public String getLeaderAddress() {
		return leaderAddress;
	}

	/**
	 * @param leaderAddress
	 *            the leaderAddress to set
	 */
	public void setLeaderAddress(String leaderAddress) {
		this.leaderAddress = leaderAddress;
	}

	/**
	 * @return the leaderPort
	 */
	public int getLeaderPort() {
		return leaderPort;
	}

	/**
	 * @param leaderPort
	 *            the leaderPort to set
	 */
	public void setLeaderPort(int leaderPort) {
		this.leaderPort = leaderPort;
	}

	/**
	 * @return the wal
	 */
	public WAL getWal() {
		return wal;
	}

	/**
	 * @param wal
	 *            the wal to set
	 */
	public void setWal(WAL wal) {
		this.wal = wal;
	}

	/**
	 * @return the client
	 */
	public WALClient getClient() {
		return client;
	}

	/**
	 * @param client
	 *            the client to set
	 */
	public void setClient(WALClient client) {
		this.client = client;
	}

	/**
	 * @return the local
	 */
	public WALClient getLocal() {
		return local;
	}

	/**
	 * @param local the local to set
	 */
	public void setLocal(WALClient local) {
		this.local = local;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Replica [routeKey=" + routeKey + ", replicaAddress=" + replicaAddress + ", replicaNodeKey="
				+ replicaNodeKey + ", replicaPort=" + replicaPort + ", leaderNodeKey=" + leaderNodeKey
				+ ", leaderAddress=" + leaderAddress + ", leaderPort=" + leaderPort + "]";
	}

}
