# Raft-Distributed-Storage-System

## System Architecture Overview
This system is built on a Multi-Raft architecture derived from the Raft consensus algorithm. In this design, data is partitioned into multiple shards, with each shard managed by an independent Raft Group. This approach enables data to be migrated among different Raft Groups for dynamic load balancing and fault recovery.

## Key Features
- **Multi-Raft data sharding**: The system partitions data into multiple shards, each maintained by its own Raft Group, which improves scalability and load balancing.
- **Data migration**: Supports migrating data across different Raft Groups for load balancing and fault recovery.
- **Linearizable KV read and write**: Provides a strongly consistent read and write API, offering linearizable semantics.
- **Performance optimization**: Enhances Raft performance using asynchronous Apply, ReadIndex, FollowerRead, PreVote, and other techniques to improve overall throughput and latency.
- **Pluggable storage engines**: Supports various storage engines (e.g., RocksDB, B+ tree, hash table) to meet diverse storage and performance requirements.

## Quick Start

### 1. Deploying a 3-Instance Raft Cluster Locally
From the project root, run the following script to deploy a 3-node Raft cluster on a single machine:
```bash
cd raft-java-example && sh deploy.sh
```
This script will create three instances (`example1`, `example2`, `example3`) under the `raft-java-example/env` directory. It will also create a `client` directory for testing the read and write functionality of the Raft cluster.

### 2. Testing Write Operations
Once the cluster is successfully deployed, you can test write operations with:
```bash
cd env/client
./bin/run_client.sh "list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" hello world
```
Here, `"list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053"` specifies the addresses of the three Raft nodes.

### 3. Testing Read Operations
To read the data you just wrote:
```bash
./bin/run_client.sh "list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" hello
```
This command retrieves the value associated with the key `hello`.

---

# How to Use the `raft-java` Library in Your Own Code

## 1. Add the Maven Dependency
Include the following in your `pom.xml`:
```xml
<dependency>
    <groupId>com.github.raftimpl.raft</groupId>
    <artifactId>raft-java-core</artifactId>
    <version>1.9.0</version>
</dependency>
```

## 2. Define Your Data Read/Write Interfaces

### Protobuf Definitions
```protobuf
message SetRequest {
    string key = 1;
    string value = 2;
}

message SetResponse {
    bool success = 1;
}

message GetRequest {
    string key = 1;
}

message GetResponse {
    string value = 1;
}
```

### Java Interface
```java
public interface ExampleService {
    Example.SetResponse set(Example.SetRequest request);
    Example.GetResponse get(Example.GetRequest request);
}
```

## 3. Implement the Server

### 3.1 Implement the `StateMachine` Interface
Your service needs a state machine that Raft will use to apply the replicated logs. It must implement the following methods:
```java
public interface StateMachine {
    /**
     * Take a snapshot of the current state machine data. 
     * This is invoked periodically on each node.
     * @param snapshotDir the directory where snapshot data should be written
     */
    void writeSnapshot(String snapshotDir);

    /**
     * Load the state machine from a given snapshot directory.
     * This is invoked when the node starts or needs to recover from a snapshot.
     * @param snapshotDir the directory containing the snapshot data
     */
    void readSnapshot(String snapshotDir);

    /**
     * Apply the given data to the state machine. 
     * This is typically the log entry data from the Raft replication layer.
     * @param dataBytes the data to be applied
     */
    void apply(byte[] dataBytes);
}
```

### 3.2 Implement Data Write and Read Logic
Your Raft-based service class will likely hold:
```java
private RaftNode raftNode;
private ExampleStateMachine stateMachine;
```

#### Data Write
```java
// Convert the request to bytes
byte[] data = request.toByteArray();

// Replicate the data to the Raft cluster
boolean success = raftNode.replicate(data, Raft.EntryType.ENTRY_TYPE_DATA);

// Build and return the response
Example.SetResponse response = Example.SetResponse.newBuilder()
    .setSuccess(success)
    .build();
return response;
```

#### Data Read
```java
// The read is served by your application-level state machine
Example.GetResponse response = stateMachine.get(request);
return response;
```

### 3.3 Server Startup
Below is a simplified example showing how to set up and start your server with Raft:

```java
// 1. Initialize RPCServer
RPCServer server = new RPCServer(localServer.getEndPoint().getPort());

// 2. Create and set your application state machine
ExampleStateMachine stateMachine = new ExampleStateMachine();

// 3. Configure Raft options (examples shown)
RaftOptions.snapshotMinLogSize = 10 * 1024;       // Min log size to trigger a snapshot
RaftOptions.snapshotPeriodSeconds = 30;          // Snapshot interval
RaftOptions.maxSegmentFileSize = 1024 * 1024;    // Max segment file size for logs

// 4. Initialize the RaftNode
RaftNode raftNode = new RaftNode(serverList, localServer, stateMachine);

// 5. Register the Raft services for inter-node consensus
RaftConsensusService raftConsensusService = new RaftConsensusServiceImpl(raftNode);
server.registerService(raftConsensusService);

// 6. Register the Raft client service
RaftClientService raftClientService = new RaftClientServiceImpl(raftNode);
server.registerService(raftClientService);

// 7. Register your application’s own service
ExampleService exampleService = new ExampleServiceImpl(raftNode, stateMachine);
server.registerService(exampleService);

// 8. Start the RPC server and initialize the Raft node
server.start();
raftNode.init();
```

By following these steps, you can stand up your own Raft-based distributed storage system, leveraging the `raft-java` library for replication and consensus. The `StateMachine` implementation and the logic for reading/writing data can then be tailored to suit your specific use case.
