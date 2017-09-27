Author: [Ambud](https://github.com/ambud)
# What is Minuteman?
Minuteman is an append only clustered WAL replication library that can be used to created a distributed AP or CP database.
It is has leader election, replica assignment and built-in check pointing so all you need to do is plug your own single node database and make a sharding choice; Minuteman will take care of the rest for you.

# Why Minuteman?
When it comes to distributed databases, everyone has their own implementation specific and tightly coupled to the database. WAL (Write Ahead Log) Replication on the other hand is a very generic feature that is used as a fundamental framework to created clustered databases. While designing clustering for Sidewinder, I realized this need to refactor the WAL replication subsystem as a generic one so it can be reused by other projects/engineers for their projects. Because Sidewinder is an append only database with no support for updates or deletes, the clustering system is also required to support this behavior. Minuteman accepts writes only, just like any replicated log system, deletes and updates can be implemented on top of this replicated log system.

# Is Minuteman for you?
If you are implementing or planning to implement a distributed database and need a library to create the clustering subsystem, this project is for you! Minuteman powers Sidewinder's clustering so it is a project under active development and tuning.

## How to use it?
To use Minuteman, you will need to make 1 major design decision and implement 2 main things. The design decision is answering the question "What is the unit of sharding in my database?" and then implement 2 main things:
1. LocalWALClient, to be used to make operations to your database
2. Protocol messages and Serializer / Deserializer for the messages you will be writing and reading from the WAL

Deciding how you want to shard your database can be tricky, e.g. Kafka it makes sense to shard a topic into partitions, elasticsearch it makes sense to shard an index and Sidewinder it makes sense to shard a database (currently) where each measurement corresponds to a shard and can't be further broken down other independent units. You can even cluster a bunch of MySQL databases with Minuteman by sharding a table, splitting by key range like HBase.

The Minuteman cluster exposes a byte[] mechanism to you for reading, writing and replicating data. You are free to decide if that byte[] is your own protocol message or just raw data. Minuteman unlike Kafka won't try to SerDe your messages. In fact, this byte[] should ideally be a batch of messages if you want performance. This is very similar to Kafka client side batching since Minuteman's leader WAL will bottleneck on the number of write requests you send to it. A 100KB byte[] works fairly well in terms of performance without running into the ISR bouncing issues.

# Design
Minuteman's design is inspired from Kafka except instead of creating a messaging broker it generalizes design to a standard Write Ahead Log replication. The difference is that sharding is not handled by Minuteman since each database technology may have different sharding entity i.e. what level it wants to shard the data (table/column/index/topic/partition etc.). The unit of work in Minuteman is a WAL which is equivalent to a Kafka partition except for there is no topic since topic is sharded entity in Kafka and Minuteman doesn't handle sharding. 

## Components

### WAL
Comparable to a Kafka partition, a WAL or Write Ahead Log is an entity in Minuteman that needs to be replicated across one or more machines. Each WAL is uniquely identified by a key / id which is used to create, track and maintain the replicas for the WAL. 

### Replica
A replica is similar to a Kafka Replica, it stores a copy of a particular WAL. When a new WAL is requested in a Minuteman cluster, the coordinator creates a Routetable entry for this WAL id and then assigns replicas to it based on a round robin assignment fashion. The coordinator also elects a leader for this WAL as well and tells all the replicas about this leader so that they can start pulling data from it. 

#### Leader
A leader Replica is the one that receives writes from the client and other replicas (followers) pull data from. This is the only Replica that receives data (from client) via a push mechanism whereas all other Replicas pull data the leader. When a WAL leader changes the coordinator notifies the replica to update their local route information and start pulling WAL from the new leader replica.

#### Follower
Follower is something (usually a Replica) that is reading from the WAL of a replica. A follower could be another Replica if this is a WAL on the Leader or it could be a Local WAL Client that is performing operations on the database instance. Follower states are tracked by the WAL, this state tracking includes the offset it's currently reading, the WAL file it's currently reading and whether or not the WAL thinks this follower is an ISR.

#### ISR
A follower that has it's offset withing "threshold" to the current write location of the WAL segment is considered an ISR or an In-Sync Replica. This threshold is configurable and needs to be tuned based on the consistency guarantees needed and the throughput needed.

### Coordinator
Coordinator is the current master of the Minuteman cluster and is responsible for all meta operations including replica leader assignment and node heartbeat maintenance. A Coordinator is a Minuteman cluster node that is elected to be a master using a leader election algorithm built on a consensus protocol Paxos (Zookeeper) or Raft (Atomix)

Coordinator announces Replica changes to the nodes in the cluster e.g. when a node fails it will more the leader ISR from that node to another available ISR and let other Replicas know where to continue reading from.

## What about CAP Theorem?
Minuteman is fundamentally an AP system if reads can be served by any replica however if reads are to be served only by WAL leader then it becomes a CP system. Consistency is handled using a water mark mechanism that prevents uncommitted WAL writes from being read by the implementing database. This water marking mechanism is baked into the replica protocol and is stored in a variable called commitOffset. This Commit Offset is the minimum committed write pointed among all ISRs therefore it is safe to read by the follower as it is guaranteed to have been replicated to all ISRs. 

