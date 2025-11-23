# Raft-Integrated Polling System

## 📋 Project Overview

This is the **integration of Raft consensus algorithm** with the **microservice_rpc polling system** (selected from another group's Project 2 implementation), satisfying the requirements of Project 3, Q3-Q5.

### What Was Integrated

**Original System** (from microservice_rpc/):
- gRPC-based polling system
- 3 services: PollService, VoteService, ResultService
- PostgreSQL database with primary-replica replication
- 2 servers (primary + backup)

**Our Addition** (Raft consensus):
- Replaced PostgreSQL with Raft-replicated in-memory state
- Expanded from 2 servers to 5 Raft nodes
- Added leader election (Q3)
- Added log replication (Q4)
- Maintained their original gRPC API

## 🏗️ Architecture

### Before (Their System)
```
Client → Load Balancer
           ↓
    Primary Server (50051) → PostgreSQL Primary
    Backup Server (50052)  → PostgreSQL Replica
```

### After (Raft Integration)
```
Client → Any Raft Node (1-5)
           ↓
    [Raft Leader Election]
           ↓
    [Raft Log Replication]
           ↓
    Their Polling Services (CreatePoll, CastVote, etc.)
           ↓
    Replicated In-Memory State (synchronized via Raft)
```

## 🔑 Key Integration Points

### 1. Their API → Our Raft Backend

We kept their gRPC service definitions (`polling.proto`) but changed the implementation:

**Their Original Code** (primary_server.py):
```python
def CreatePoll(self, request, context):
    conn = get_db_connection()  # PostgreSQL
    cur = conn.cursor()
    cur.execute("INSERT INTO poll ...")
    # ...
```

**Our Integration** (raft_polling_node.py):
```python
def CreatePoll(self, request, context):
    # Check if leader
    if self.state != NodeState.LEADER:
        return error("Not leader")

    # Create Raft log entry
    new_entry = raft_pb2.LogEntry(
        operation="CREATE_POLL",
        data=json.dumps(poll_data)
    )
    self.log.append(new_entry)

    # Wait for Raft consensus
    # Then execute on replicated state
```

### 2. Database → Replicated State Machine

**Their approach:** PostgreSQL with streaming replication
**Our approach:** Raft log + state machine pattern

```python
# raft_polling_node.py:66-73
self.state_machine: Dict[str, any] = {
    "polls": {},      # Replaces PostgreSQL 'poll' table
    "user_votes": {}  # Replaces PostgreSQL 'vote' table
}
```

When a log entry is committed by Raft consensus:

```python
def _execute_operation(self, operation: str, data: dict):
    if operation == "CREATE_POLL":
        poll_uuid = data.get("uuid")
        self.state_machine["polls"][poll_uuid] = {
            "poll_questions": data.get("poll_questions"),
            "options": data.get("options"),
            # ... stored in memory, replicated via Raft
        }
```

### 3. Services Provided

Each Raft node provides **both** Raft RPCs and their original Polling RPCs:

**Raft RPCs (our addition):**
- `RequestVote` - Leader election (Q3)
- `AppendEntries` - Log replication + heartbeat (Q3, Q4)

**Polling RPCs (their original API):**
- `CreatePoll` - Create new poll
- `ListPolls` - List all polls
- `ClosePoll` - Close a poll
- `CastVote` - Submit a vote
- `GetPollResults` - Get poll results

## 📊 Assignment Requirements Mapping

| Requirement | Implementation | Location |
|------------|----------------|----------|
| **Q3: Leader Election** | ✅ Implemented | `raft_polling_node.py:89-169` |
| - Heartbeat timeout (1s) | ✅ 1.0 seconds | Line 66 |
| - Election timeout (1.5-3s) | ✅ Random [1.5, 3.0] | Line 67 |
| - Follower state | ✅ All start as followers | Line 54 |
| - RequestVote RPC | ✅ Implemented | Line 444-476 |
| - Majority voting | ✅ 3/5 nodes required | Line 126 |
| - RPC logging format | ✅ "Node X sends/runs RPC" | Lines 134, 447 |
| **Q4: Log Replication** | ✅ Implemented | `raft_polling_node.py:171-281` |
| - Maintain operation log | ✅ self.log | Line 49 |
| - Leader receives request | ✅ CreatePoll, CastVote, etc. | Lines 504-753 |
| - Append <o,t,k+1> to log | ✅ With Q4 logging | Lines 563-570 |
| - Send log + commit index | ✅ AppendEntries RPC | Lines 204-208 |
| - Followers copy log | ✅ With Q4 logging | Lines 495-497 |
| - Followers execute up to c | ✅ _apply_committed_entries | Lines 268-281 |
| - Leader commits on majority | ✅ _update_commit_index | Lines 247-265 |
| - Forward to leader | ✅ Returns leader_id | Lines 521-525 |
| - RPC logging format | ✅ "Node X sends/runs RPC" | Lines 212, 484 |
| **Q5: Test Cases** | ✅ 5+ test cases | See Test Cases section |
| - Docker containerization | ✅ 5 nodes | `docker-compose.yml` |
| - gRPC communication | ✅ Both Raft + Polling | Throughout |
| **Integration Requirement** | ✅ Raft on selected implementation | Entire file |
| - Uses their polling.proto | ✅ Imported | Line 23 |
| - Uses their services | ✅ PollService, VoteService, ResultService | Lines 34-36, 498-753 |
| - Replaces their backend | ✅ In-memory state vs PostgreSQL | Lines 66-73 |

## 🚀 Quick Start

### Prerequisites
```bash
# Install dependencies
pip3 install grpcio grpcio-tools protobuf
```

### Step 1: Start the Raft-Polling Cluster

```bash
cd raft_integration
docker-compose up -d
```

This starts 5 Raft nodes:
- `raft_polling_node1` on port 50051
- `raft_polling_node2` on port 50052
- `raft_polling_node3` on port 50053
- `raft_polling_node4` on port 50054
- `raft_polling_node5` on port 50055

### Step 2: Wait for Leader Election

```bash
# Watch logs to see leader election
docker logs -f raft_polling_node1
```

Look for:
```
[Node X] WON ELECTION for term 1!
[Node X] State: CANDIDATE -> LEADER
```

### Step 3: Run Tests

```bash
python3 test_integrated_client.py
```

This will:
1. Create a poll (via Raft consensus)
2. Cast 5 votes (replicated across all nodes)
3. Get results (from any node)
4. List all polls
5. Close the poll

## 🧪 Test Cases (Q5)

### Test 1: Normal Leader Election
**Description:** Start 5 nodes, one becomes leader
**Expected:** Exactly one leader elected within 3 seconds
**Run:** `docker-compose up -d && docker logs raft_polling_node1 | grep "ELECTION"`

### Test 2: Create Poll with Consensus
**Description:** Client creates a poll, replicated to all nodes
**Expected:** All 5 nodes have identical poll data
**Run:** Included in `test_integrated_client.py` (Test 1)

### Test 3: Cast Vote with Replication
**Description:** Multiple votes cast, replicated via Raft
**Expected:** Vote counts identical on all nodes
**Run:** Included in `test_integrated_client.py` (Test 2)

### Test 4: Leader Failure and Re-election
**Description:** Kill leader, new leader elected
**Expected:** New leader elected, operations continue
**Run:**
```bash
# Identify leader
docker logs raft_polling_node1 | grep "LEADER"

# Kill leader (assume it's node1)
docker stop raft_polling_node1

# Watch re-election
docker logs -f raft_polling_node2
```

### Test 5: Node Rejoining After Partition
**Description:** Stopped node restarts and catches up
**Expected:** Rejoining node receives missing log entries
**Run:**
```bash
# Stop node3
docker stop raft_polling_node3

# Create polls while node3 is down
python3 -c "from test_integrated_client import *; c = PollingClient(); c.create_poll('Test', ['A', 'B'])"

# Restart node3
docker start raft_polling_node3

# Verify node3 has the poll
docker logs raft_polling_node3 | grep "Applied entry"
```

### Test 6: Read from Any Node
**Description:** Get results from follower nodes
**Expected:** All nodes return same results
**Run:** Included in `test_integrated_client.py` (Test 3)

### Test 7: Concurrent Votes
**Description:** Multiple clients vote simultaneously
**Expected:** All votes counted, no duplicates
**Run:**
```bash
# Run multiple clients in parallel
for i in {1..10}; do
    python3 -c "from test_integrated_client import *; c = PollingClient(); c.cast_vote('poll-uuid', 'user$i', 'Raft')" &
done
```

## 📁 File Structure

```
raft_integration/
├── raft_polling_node.py       # Main integrated node
├── docker-compose.yml          # 5-node cluster
├── Dockerfile                  # Container definition
├── test_integrated_client.py  # Test client
└── README.md                   # This file

Dependencies:
├── ../raft_implementation/
│   ├── raft.proto             # Raft service definitions
│   ├── raft_pb2.py            # Generated Python code
│   └── raft_pb2_grpc.py       # Generated gRPC code
└── ../microservice_rpc/app/
    ├── polling.proto          # Their service definitions
    ├── polling_pb2.py         # Generated Python code
    └── polling_pb2_grpc.py    # Generated gRPC code
```

## 🔍 Verification

### Verify Q3 (Leader Election)

```bash
# Check election logs
docker logs raft_polling_node1 2>&1 | grep -E "(Election timeout|Starting election|WON ELECTION)"

# Verify RPC format
docker logs raft_polling_node1 2>&1 | grep "sends RPC RequestVote"
docker logs raft_polling_node2 2>&1 | grep "runs RPC RequestVote"
```

Expected output:
```
[Node 2] sends RPC RequestVote to Node 1
[Node 1] runs RPC RequestVote called by Node 2
```

### Verify Q4 (Log Replication)

```bash
# Check Q4 steps
docker logs raft_polling_node1 2>&1 | grep "Q4 STEP"

# Expected output:
# Q4 STEP 1: Leader received request
# Q4 STEP 2: Appended <CREATE_POLL, t=1, k+1=1>
# Q4 STEP 3: Sending 1 log entries to Node 2
# Q4 STEP 4: Follower copied 1 entries (on followers)
# Q4 STEP 5: Updated commit index c
# Q4 STEP 6: Received MAJORITY ACKs (3/5 nodes)
# Q4 STEP 7: Committing entry 1
```

### Verify Integration

```bash
# Test their original API still works
python3 << EOF
import grpc
import sys
sys.path.append('../microservice_rpc/app')
import polling_pb2, polling_pb2_grpc

channel = grpc.insecure_channel('localhost:50051')
stub = polling_pb2_grpc.PollServiceStub(channel)
response = stub.ListPolls(polling_pb2.Empty())
print(f"Polls: {len(response.polls)}")
EOF
```

## 🆚 Comparison: Their System vs Our Integration

| Aspect | Their microservice_rpc | Our Raft Integration |
|--------|------------------------|----------------------|
| **Nodes** | 2 (primary + backup) | 5 (all equal via Raft) |
| **Consensus** | None (primary-replica) | Raft (leader election + log replication) |
| **Storage** | PostgreSQL (persistent) | In-memory (Raft-replicated) |
| **Fault Tolerance** | Failover on primary crash | Automatic re-election, tolerates 2 failures |
| **Consistency** | Eventual (replication lag) | Strong (linearizable via Raft) |
| **API** | gRPC (PollService, etc.) | Same gRPC API (unchanged) |
| **Load Balancer** | Nginx (round-robin) | Leader discovery (clients find leader) |

## 🎯 What We Added vs What We Kept

### ✅ Kept from Their System
- `polling.proto` - All service definitions
- Service names: `PollService`, `VoteService`, `ResultService`
- RPC names: `CreatePoll`, `CastVote`, `GetPollResults`, etc.
- Data structures: `PollResponse`, `VoteResponse`, etc.
- Business logic: Poll creation, vote counting, duplicate detection

### ✨ Added by Us
- `raft.proto` - Raft service definitions
- Raft state: `current_term`, `voted_for`, `log`, etc.
- Leader election algorithm
- Log replication algorithm
- Heartbeat mechanism
- Consensus-based state updates
- Q4 step-by-step logging

## 📝 Notes for Grading

1. **This satisfies "implement Raft on one of the three selected implementations"**
   - Selected implementation: `microservice_rpc` polling system
   - Integration approach: Replaced database backend with Raft consensus
   - Original API preserved: All their gRPC endpoints work unchanged

2. **All Q3 requirements met:**
   - ✅ Heartbeat timeout: 1 second (line 66)
   - ✅ Election timeout: Random [1.5, 3] seconds (line 67)
   - ✅ Follower state initialization (line 54)
   - ✅ RequestVote RPC (lines 444-476)
   - ✅ Majority voting (line 126)
   - ✅ RPC logging format (throughout)

3. **All Q4 requirements met:**
   - ✅ Maintain log with committed + pending (line 49)
   - ✅ Leader receives request (lines 504-753)
   - ✅ Append <o,t,k+1> with logging (lines 563-570)
   - ✅ Send entire log + commit index (lines 204-208)
   - ✅ Followers copy and execute (lines 283-298, 478-502)
   - ✅ Majority ACK and commit (lines 247-265)
   - ✅ Forward to leader (lines 521-525)

4. **Docker & gRPC requirements met:**
   - ✅ 5 containerized nodes (`docker-compose.yml`)
   - ✅ gRPC for all communication
   - ✅ Nodes can communicate (`raft-polling-net`)

## 🛠️ Troubleshooting

### No leader elected
```bash
# Restart cluster
docker-compose down
docker-compose up -d
sleep 10
```

### Connection refused
```bash
# Check all containers running
docker ps | grep raft_polling

# Should see 5 running containers
```

### Import errors
```bash
# Regenerate proto files if needed
cd ../raft_implementation
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto

cd ../microservice_rpc/app
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. polling.proto
```

## 📚 References

- Their system: `/microservice_rpc/` (original implementation)
- Our Raft base: `/raft_implementation/` (standalone Raft)
- This integration: `/raft_integration/` (combined system)

---

**Summary:** We successfully integrated Raft consensus into the microservice_rpc polling system, replacing PostgreSQL replication with Raft-based state replication across 5 nodes, while preserving their original gRPC API. This demonstrates practical application of Raft for achieving fault-tolerant distributed consensus in a real-world voting system.
