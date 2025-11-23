# Q4 Log Replication - Presentation Guide

This guide will help you demonstrate the Q4 log replication implementation for your presentation.

## Quick Start

### Step 1: Start the Raft Cluster

```bash
cd raft_implementation
docker-compose up -d
```

Wait 10 seconds for the cluster to elect a leader.

### Step 2: Run the Q4 Demonstration

```bash
python3 test_q4_demonstration.py
```

This will submit 3 operations and clearly show all Q4 steps in the output.

## Detailed Q4 Flow (What to Show in Your Presentation)

The enhanced logging now shows **all required Q4 steps**:

### Q4 STEP 1: Leader Receives Request
**Log message:**
```
[Node X] runs RPC ClientRequest called by client
[Node X] ✓ I AM LEADER - Processing client request
[Node X] Q4 STEP 1: Leader received request for operation 'CREATE_POLL'
```

### Q4 STEP 2: Leader Appends <o, t, k+1> to Log
**Log message:**
```
[Node X] Q4 STEP 2: Appended <CREATE_POLL, t=1, k+1=1> to log
[Node X]           Log size: 1, Commit index c=0
[Node X]           Entry details: index=1, term=1, op=CREATE_POLL
```

This shows the operation `o`, term `t`, and index `k+1`.

### Q4 STEP 3: Leader Sends Log to Followers
**Log message:**
```
[Node X] Q4 STEP 3: Will send log to all followers on next heartbeat...
[Node X] Q4 STEP 3: Sending 1 log entries to Node Y
[Node X]            Entries: ['idx=1']
[Node X]            With commit index c=0
[Node X] sends RPC AppendEntries (1 entries) to Node Y
```

### Q4 STEP 4: Followers Copy Log
**Log message on follower:**
```
[Node Y] runs RPC AppendEntries (1 entries) called by Node X
[Node Y] Q4 STEP 4: Follower copied 1 entries to log
[Node Y]            Log size now: 1
[Node Y]            - Entry 1: CREATE_POLL (term 1)
```

### Q4 STEP 5: Followers Update Commit Index
**Log message on follower:**
```
[Node Y] Q4 STEP 5: Updated commit index c: 0 → 1
[Node Y]            Will execute operations up to index 1
```

### Q4 STEP 6: Leader Receives Majority ACKs
**Log message on leader:**
```
[Node X] Q4: Received ACK from Node 2 (replicated up to 1)
[Node X] Q4: Received ACK from Node 3 (replicated up to 1)
[Node X] Q4 STEP 6: Received MAJORITY ACKs (3/5 nodes)
```

This shows majority consensus (3 out of 5 nodes).

### Q4 STEP 7: Leader Commits Entry
**Log message on leader:**
```
[Node X] Q4 STEP 7: Committing entry 1 (was pending, now committed)
[Node X]            Incrementing c: 0 → 1
```

### Final: Operation Executed
**Log message on all nodes:**
```
[Node X] Q4 EXECUTE: Applied entry 1: CREATE_POLL
[Node X]             Data: {"question": "What is your favorite..."}
[Node X] Q4 FINAL: Operation committed at index 1
[Node X]          Commit index c incremented: 0 → 1
```

## How to Capture Logs for Your Presentation

### Option 1: Watch Live Logs

Open 3 terminal windows and run:

**Terminal 1 (Leader logs):**
```bash
docker logs -f raft_node1 | grep "Q4"
```

**Terminal 2 (Follower logs):**
```bash
docker logs -f raft_node2 | grep "Q4"
```

**Terminal 3 (Test output):**
```bash
python3 test_q4_demonstration.py
```

### Option 2: Capture Complete Logs

```bash
# Run the test
python3 test_q4_demonstration.py

# Save logs to files
docker logs raft_node1 > leader_logs.txt
docker logs raft_node2 > follower1_logs.txt
docker logs raft_node3 > follower2_logs.txt

# Extract only Q4-related logs
grep "Q4" leader_logs.txt > q4_leader.txt
grep "Q4" follower1_logs.txt > q4_follower1.txt
grep "Q4" follower2_logs.txt > q4_follower2.txt
```

### Option 3: Use the Helper Script

```bash
./capture_q4_logs.sh
```

This will run the test and automatically capture all Q4 logs to files.

## Screenshots to Take for Your Report

1. **Screenshot 1: Test Execution**
   - Run `python3 test_q4_demonstration.py`
   - Capture the output showing all 3 operations succeeding

2. **Screenshot 2: Leader Q4 Steps**
   - Run `docker logs raft_node1 | grep "Q4"`
   - Capture showing STEP 1, STEP 2, STEP 3, STEP 6, STEP 7

3. **Screenshot 3: Follower Q4 Steps**
   - Run `docker logs raft_node2 | grep "Q4"`
   - Capture showing STEP 4, STEP 5, EXECUTE

4. **Screenshot 4: RPC Messages**
   - Run `docker logs raft_node1 | grep "RPC"`
   - Show the required format: "Node X sends RPC Y to Node Z"

## Mapping to Assignment Requirements

Your implementation satisfies all Q4 requirements:

✅ **Leader receives request** → Q4 STEP 1 logs
✅ **Leader appends <o, t, k+1>** → Q4 STEP 2 logs show operation, term, and index
✅ **Leader sends entire log with commit index c** → Q4 STEP 3 logs
✅ **Followers copy log** → Q4 STEP 4 logs
✅ **Followers update commit index c** → Q4 STEP 5 logs
✅ **Leader receives majority ACKs** → Q4 STEP 6 logs show 3/5 nodes
✅ **Leader commits entry and increments c** → Q4 STEP 7 logs
✅ **All nodes execute operation** → Q4 EXECUTE logs

## RPC Logging Format

All RPC calls follow the required format:

**Sender side:**
```
[Node X] sends RPC RequestVote to Node Y
[Node X] sends RPC AppendEntries (1 entries) to Node Y
```

**Receiver side:**
```
[Node Y] runs RPC RequestVote called by Node X
[Node Y] runs RPC AppendEntries (1 entries) called by Node X
[Node Y] runs RPC ClientRequest called by client
```

## Common Issues

### No leader elected
```bash
# Restart the cluster
docker-compose down
docker-compose up -d
# Wait 10 seconds
python3 test_q4_demonstration.py
```

### Connection refused
```bash
# Check if all containers are running
docker ps

# You should see 5 containers: raft_node1 through raft_node5
# If not, restart:
docker-compose up -d
```

### Operations timing out
- Increase the wait time in test (already set to 10s)
- Check Docker logs for errors: `docker logs raft_node1`

## Tips for Your Presentation

1. **Run the test first** to verify everything works
2. **Capture screenshots** of the logs showing Q4 steps
3. **Highlight the key steps** in your slides:
   - Show STEP 1-2 (leader side)
   - Show STEP 3 (replication)
   - Show STEP 4-5 (follower side)
   - Show STEP 6-7 (commitment)
4. **Explain the consensus**: Point out that 3/5 nodes = majority
5. **Show fault tolerance**: Explain that the system can tolerate 2 node failures

## Cleanup

After your presentation:

```bash
docker-compose down
```

This will stop all Raft nodes.
