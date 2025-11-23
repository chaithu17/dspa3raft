#!/bin/bash
# Helper script to capture Q4 demonstration logs for presentation

echo "==============================================="
echo "Q4 Log Replication - Log Capture Script"
echo "==============================================="
echo ""

# Create logs directory
mkdir -p q4_logs
cd q4_logs

echo "Step 1: Checking if Docker containers are running..."
if ! docker ps | grep -q "raft_node"; then
    echo "ERROR: Docker containers not running!"
    echo "Please run: docker-compose up -d"
    exit 1
fi
echo "✓ Containers are running"
echo ""

# Clear previous logs
docker-compose logs --no-color > /dev/null 2>&1

echo "Step 2: Running Q4 demonstration test..."
echo "This will submit 3 operations to the cluster"
echo ""

# Run the test
cd ..
python3 test_q4_demonstration.py > q4_logs/test_output.txt 2>&1

echo ""
echo "Step 3: Capturing logs from all nodes..."

# Capture full logs
docker logs raft_node1 > q4_logs/node1_full.log 2>&1
docker logs raft_node2 > q4_logs/node2_full.log 2>&1
docker logs raft_node3 > q4_logs/node3_full.log 2>&1
docker logs raft_node4 > q4_logs/node4_full.log 2>&1
docker logs raft_node5 > q4_logs/node5_full.log 2>&1

echo "✓ Full logs captured"
echo ""

echo "Step 4: Extracting Q4-specific logs..."

# Extract Q4 logs
grep "Q4" q4_logs/node1_full.log > q4_logs/node1_q4.log
grep "Q4" q4_logs/node2_full.log > q4_logs/node2_q4.log
grep "Q4" q4_logs/node3_full.log > q4_logs/node3_q4.log
grep "Q4" q4_logs/node4_full.log > q4_logs/node4_q4.log
grep "Q4" q4_logs/node5_full.log > q4_logs/node5_q4.log

echo "✓ Q4 logs extracted"
echo ""

echo "Step 5: Extracting RPC logs..."

# Extract RPC logs
grep "RPC" q4_logs/node1_full.log > q4_logs/node1_rpc.log
grep "RPC" q4_logs/node2_full.log > q4_logs/node2_rpc.log
grep "RPC" q4_logs/node3_full.log > q4_logs/node3_rpc.log
grep "RPC" q4_logs/node4_full.log > q4_logs/node4_rpc.log
grep "RPC" q4_logs/node5_full.log > q4_logs/node5_rpc.log

echo "✓ RPC logs extracted"
echo ""

echo "Step 6: Creating summary report..."

# Create summary
cat > q4_logs/SUMMARY.txt <<EOF
=================================================================
Q4 LOG REPLICATION - CAPTURED LOGS SUMMARY
=================================================================

Test Execution Time: $(date)

Files Generated:
-----------------------------------------------------------------
test_output.txt     - Complete test execution output
node1_full.log      - Node 1 complete logs
node2_full.log      - Node 2 complete logs
node3_full.log      - Node 3 complete logs
node4_full.log      - Node 4 complete logs
node5_full.log      - Node 5 complete logs

node1_q4.log        - Node 1 Q4-specific logs (for presentation)
node2_q4.log        - Node 2 Q4-specific logs
node3_q4.log        - Node 3 Q4-specific logs
node4_q4.log        - Node 4 Q4-specific logs
node5_q4.log        - Node 5 Q4-specific logs

node1_rpc.log       - Node 1 RPC messages
node2_rpc.log       - Node 2 RPC messages
node3_rpc.log       - Node 3 RPC messages
node4_rpc.log       - Node 4 RPC messages
node5_rpc.log       - Node 5 RPC messages

=================================================================
Q4 STEPS TO LOOK FOR IN LOGS:
=================================================================

STEP 1: Leader receives request
  → Look in: node1_q4.log (or whichever node is leader)
  → Search for: "Q4 STEP 1: Leader received request"

STEP 2: Leader appends <o, t, k+1> to log
  → Look in: node1_q4.log
  → Search for: "Q4 STEP 2: Appended <"

STEP 3: Leader sends log to followers
  → Look in: node1_q4.log
  → Search for: "Q4 STEP 3: Sending"

STEP 4: Followers copy log
  → Look in: node2_q4.log, node3_q4.log, etc.
  → Search for: "Q4 STEP 4: Follower copied"

STEP 5: Followers update commit index
  → Look in: node2_q4.log, node3_q4.log, etc.
  → Search for: "Q4 STEP 5: Updated commit index"

STEP 6: Leader receives majority ACKs
  → Look in: node1_q4.log
  → Search for: "Q4 STEP 6: Received MAJORITY ACKs"

STEP 7: Leader commits entry
  → Look in: node1_q4.log
  → Search for: "Q4 STEP 7: Committing entry"

EXECUTE: Operations executed on all nodes
  → Look in: all node*_q4.log files
  → Search for: "Q4 EXECUTE: Applied entry"

=================================================================
RECOMMENDED FOR PRESENTATION:
=================================================================

1. Show test_output.txt - demonstrates successful operation
2. Show node1_q4.log - demonstrates leader-side Q4 steps (1,2,3,6,7)
3. Show node2_q4.log - demonstrates follower-side Q4 steps (4,5)
4. Show node1_rpc.log - demonstrates RPC message format

=================================================================
EOF

echo "✓ Summary created"
echo ""

echo "==============================================="
echo "SUCCESS! All logs captured"
echo "==============================================="
echo ""
echo "Logs saved in: ./q4_logs/"
echo ""
echo "Quick views:"
echo "  Test output:     cat q4_logs/test_output.txt"
echo "  Leader Q4 logs:  cat q4_logs/node1_q4.log"
echo "  Follower Q4:     cat q4_logs/node2_q4.log"
echo "  RPC messages:    cat q4_logs/node1_rpc.log"
echo "  Summary:         cat q4_logs/SUMMARY.txt"
echo ""
echo "For your presentation, use the files in q4_logs/"
echo "==============================================="
