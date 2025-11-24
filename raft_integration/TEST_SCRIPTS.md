# Test Scripts for Raft-Integrated Polling System

This directory contains comprehensive test scripts for validating the Raft consensus implementation integrated with the microservice polling system.

## Available Test Scripts

### 1. Leader Election Test
**File:** `test_leader_election.py`

**Purpose:** Validates Q3 leader election requirements

**What it tests:**
- Cluster startup and initialization
- Leader election process
- Heartbeat mechanism (1 second intervals)
- Election timeout (random 1.5-3 seconds)
- RequestVote RPC format
- Exactly one leader elected

**Usage:**
```bash
python3 test_leader_election.py
```

**Expected output:**
- Shows real-time election events
- Identifies elected leader
- Verifies RPC format
- Shows heartbeat messages
- Displays comprehensive summary

---

### 2. Q5 Comprehensive Test Cases
**File:** `test_q5_cases.py`

**Purpose:** Validates all Q5 test requirements (6 test cases)

**What it tests:**

#### Test 2: Create Poll with Consensus
- Creates poll on Node 1
- Verifies all 5 nodes have identical poll data
- Validates Raft replication

#### Test 3: Cast Vote with Replication
- Casts votes from different nodes
- Verifies vote counts identical on all nodes
- Validates consensus mechanism

#### Test 4: Leader Failure and Re-election
- Stops current leader
- Verifies new leader elected
- Validates fault tolerance

#### Test 5: Node Rejoining After Partition
- Stops Node 3
- Creates poll while Node 3 is down
- Restarts Node 3
- Verifies Node 3 catches up on missed entries

#### Test 6: Read from Any Node
- Creates poll and casts votes
- Reads results from all 5 nodes
- Verifies all nodes return identical results

#### Test 7: Concurrent Votes
- 10 concurrent votes from multiple threads
- Distributed across all nodes
- Verifies no duplicates, correct counting

**Usage:**
```bash
python3 test_q5_cases.py
```

**Expected output:**
- Individual test results with PASS/FAIL
- Detailed logs for each test
- Final summary showing X/6 tests passed

---

### 3. Basic Integration Tests
**File:** `test_integrated_client.py`

**Purpose:** Basic demonstration of polling API with Raft

**What it tests:**
- CreatePoll RPC
- CastVote RPC
- GetPollResults RPC
- ListPolls RPC
- ClosePoll RPC

**Usage:**
```bash
python3 test_integrated_client.py
```

---

### 4. Shell Script Tests
**File:** `test_election.sh`

**Purpose:** Simple shell script version of leader election test

**Usage:**
```bash
./test_election.sh
```

---

### 5. Debug Script
**File:** `debug_cluster.py`

**Purpose:** Troubleshoot cluster issues

**What it shows:**
- Container status
- Recent logs from all 5 nodes
- Initialization messages
- Errors and exceptions
- Docker compose status

**Usage:**
```bash
python3 debug_cluster.py
```

---

## Running Tests

### Prerequisites
1. Docker and docker-compose installed
2. All containers running:
   ```bash
   docker-compose up -d
   ```

### Full Test Suite
Run all tests in sequence:

```bash
# 1. Test leader election (Q3)
python3 test_leader_election.py

# 2. Test Q5 requirements
python3 test_q5_cases.py

# 3. Basic integration test
python3 test_integrated_client.py
```

### Quick Verification
```bash
# Check if all containers are running
docker ps --filter "name=raft_polling"

# View logs from leader
docker logs raft_polling_node1 | grep -E "WON ELECTION|LEADER"

# Check for errors
docker logs raft_polling_node1 2>&1 | grep -i error
```

---

## Test Requirements Coverage

### Q3: Leader Election
✅ **test_leader_election.py**
- Heartbeat timeout: 1 second
- Election timeout: Random [1.5, 3] seconds
- All nodes start as FOLLOWER
- RequestVote RPC
- Exactly one leader elected
- Correct RPC logging format

### Q4: Log Replication
✅ **test_q5_cases.py** (Tests 2, 3, 5)
- 7 steps clearly logged
- AppendEntries RPC
- Forwarding to leader (any node can receive requests)

### Q5: Test Cases
✅ **test_q5_cases.py**
- Test 2: Create Poll with Consensus
- Test 3: Cast Vote with Replication
- Test 4: Leader Failure and Re-election
- Test 5: Node Rejoining After Partition
- Test 6: Read from Any Node
- Test 7: Concurrent Votes

---

## Troubleshooting

### Containers not starting
```bash
docker-compose down
docker-compose up -d --build
```

### No leader elected
```bash
# Check election timeout in logs
docker logs raft_polling_node1 2>&1 | grep -i "election timeout"

# Check for network issues
docker network inspect raft_integration_raft-polling-net
```

### Tests timing out
```bash
# Increase wait times in test scripts
# Check container health
docker stats
```

### Port conflicts
```bash
# Stop all raft containers
docker stop $(docker ps -q --filter "name=raft")
docker rm $(docker ps -aq --filter "name=raft")
```

---

## Expected Test Results

### Successful Test Run
```
TEST 2: PASSED - All 5 nodes have identical poll data
TEST 3: PASSED - All 5 nodes have identical vote counts
TEST 4: PASSED - New leader elected after failure
TEST 5: PASSED - Node 3 caught up on missed entries
TEST 6: PASSED - All nodes return identical results
TEST 7: PASSED - 10 votes counted, no duplicates

Total: 6/6 tests passed
ALL TESTS PASSED - Q5 REQUIREMENTS MET
```

---

## Additional Resources

- **Docker logs:** `docker logs raft_polling_nodeX`
- **Follow logs:** `docker logs -f raft_polling_nodeX`
- **Cluster status:** `docker-compose ps`
- **Restart cluster:** `docker-compose restart`

---

## Notes

- Tests should be run with a fresh cluster for best results
- Each test waits for replication (typically 2-5 seconds)
- Some tests intentionally stop/start containers
- All tests verify consensus and replication
- RPC format matches specification exactly
