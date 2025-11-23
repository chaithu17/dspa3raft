#!/usr/bin/env python3
"""
Q4 Demonstration Test Case - For Presentation
This test clearly shows all Q4 log replication steps with detailed logging
"""

import grpc
import json
import time
import sys

import raft_pb2
import raft_pb2_grpc


class Q4Demonstrator:
    """Demonstration test for Q4 log replication"""

    def __init__(self):
        self.nodes = {
            1: "localhost:50051",
            2: "localhost:50052",
            3: "localhost:50053",
            4: "localhost:50054",
            5: "localhost:50055"
        }

    def print_section(self, title):
        """Print section header"""
        print("\n" + "="*70)
        print(f"  {title}")
        print("="*70 + "\n")

    def find_leader(self):
        """Find the current leader"""
        print("[Demonstrator] Discovering current leader...")

        for node_id, addr in self.nodes.items():
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = raft_pb2_grpc.RaftNodeStub(channel)
                    request = raft_pb2.ClientRequestMessage(
                        operation="PING",
                        data=json.dumps({})
                    )
                    response = stub.ClientRequest(request, timeout=1.0)

                    if response.success or response.leader_id == node_id:
                        print(f"[Demonstrator] ✓ Found leader: Node {node_id}\n")
                        return node_id
                    elif response.leader_id > 0:
                        print(f"[Demonstrator] ✓ Found leader: Node {response.leader_id}\n")
                        return response.leader_id
            except:
                pass

        print("[Demonstrator] ✗ No leader found\n")
        return None

    def submit_operation(self, operation, data, description):
        """Submit an operation and show the Q4 flow"""
        leader = self.find_leader()
        if not leader:
            print("[Demonstrator] ERROR: No leader available!")
            return False

        self.print_section(f"Q4 DEMONSTRATION: {description}")

        print(f"[Demonstrator] Submitting operation to Node {leader} (current leader)")
        print(f"[Demonstrator] Operation: {operation}")
        print(f"[Demonstrator] Data: {json.dumps(data, indent=2)}")
        print("\n" + "-"*70)
        print("WATCH FOR THE FOLLOWING Q4 STEPS IN THE LOGS:")
        print("-"*70)
        print("STEP 1: Leader receives request")
        print("STEP 2: Leader appends <o, t, k+1> to its log")
        print("STEP 3: Leader sends log entries to all followers")
        print("STEP 4: Followers copy log entries")
        print("STEP 5: Followers update commit index c")
        print("STEP 6: Leader receives majority ACKs")
        print("STEP 7: Leader commits the entry")
        print("FINAL:  Operation is executed on all nodes")
        print("-"*70 + "\n")

        try:
            addr = self.nodes[leader]
            with grpc.insecure_channel(addr) as channel:
                stub = raft_pb2_grpc.RaftNodeStub(channel)

                request = raft_pb2.ClientRequestMessage(
                    operation=operation,
                    data=json.dumps(data)
                )

                print(f"[Demonstrator] >>> Sending request to Node {leader}...")
                print(f"[Demonstrator] >>> Waiting for consensus and commitment...\n")

                response = stub.ClientRequest(request, timeout=10.0)

                print("\n" + "-"*70)
                print("RESPONSE FROM LEADER:")
                print("-"*70)

                if response.success:
                    print(f"[Demonstrator] ✓ SUCCESS!")
                    print(f"[Demonstrator]   Message: {response.message}")
                    print(f"[Demonstrator]   Result: {response.result}")
                    print(f"[Demonstrator]   Leader: Node {response.leader_id}")
                    print("\n✓ The operation has been successfully replicated and committed!")
                    print("✓ Check the Docker logs above to see all Q4 steps in detail.")
                    return True
                else:
                    print(f"[Demonstrator] ✗ FAILED")
                    print(f"[Demonstrator]   Message: {response.message}")
                    return False

        except Exception as e:
            print(f"\n[Demonstrator] ✗ Exception occurred: {e}")
            return False

    def run_demonstration(self):
        """Run the Q4 demonstration"""
        print("\n" + "#"*70)
        print("#" + " "*68 + "#")
        print("#" + "  Q4 LOG REPLICATION - DEMONSTRATION".center(68) + "#")
        print("#" + "  Raft Consensus Algorithm".center(68) + "#")
        print("#" + " "*68 + "#")
        print("#"*70)

        print("\n[Demonstrator] This demonstration will show the complete Q4 flow:")
        print("[Demonstrator] - Leader receives client request")
        print("[Demonstrator] - Leader appends to log and replicates")
        print("[Demonstrator] - Followers acknowledge replication")
        print("[Demonstrator] - Leader commits after majority ACK")
        print("[Demonstrator] - All nodes execute the committed operation")

        # Wait a bit for cluster to stabilize
        print("\n[Demonstrator] Waiting for cluster to stabilize...")
        time.sleep(2)

        # Test Case 1: Create a poll
        success1 = self.submit_operation(
            "CREATE_POLL",
            {
                "question": "What is your favorite distributed consensus algorithm?",
                "options": ["Raft", "Paxos", "Zab", "Viewstamped Replication"]
            },
            "Creating a New Poll"
        )

        time.sleep(3)  # Wait between operations

        # Test Case 2: Submit a vote
        success2 = self.submit_operation(
            "VOTE",
            {
                "poll_id": "poll_1",
                "option": "Raft"
            },
            "Submitting a Vote"
        )

        time.sleep(3)

        # Test Case 3: Another vote
        success3 = self.submit_operation(
            "VOTE",
            {
                "poll_id": "poll_1",
                "option": "Raft"
            },
            "Submitting Another Vote"
        )

        # Summary
        self.print_section("DEMONSTRATION SUMMARY")

        results = [
            ("Test 1: Create Poll", success1),
            ("Test 2: Submit Vote 1", success2),
            ("Test 3: Submit Vote 2", success3)
        ]

        passed = sum(1 for _, result in results if result)
        total = len(results)

        for test_name, result in results:
            status = "✓ SUCCESS" if result else "✗ FAILED"
            print(f"  {test_name}: {status}")

        print(f"\n  Total: {passed}/{total} operations successfully replicated and committed")
        print("\n" + "="*70)
        print("IMPORTANT NOTES FOR YOUR PRESENTATION:")
        print("="*70)
        print("1. Look at the Docker logs to see the detailed Q4 steps:")
        print("   docker logs raft_node1  # Leader logs")
        print("   docker logs raft_node2  # Follower logs")
        print("   docker logs raft_node3  # Follower logs")
        print("")
        print("2. Key Q4 steps visible in logs:")
        print("   - 'Q4 STEP 1: Leader received request'")
        print("   - 'Q4 STEP 2: Appended <o, t, k+1> to log'")
        print("   - 'Q4 STEP 3: Sending N log entries to Node X'")
        print("   - 'Q4 STEP 4: Follower copied N entries to log'")
        print("   - 'Q4 STEP 5: Updated commit index c'")
        print("   - 'Q4 STEP 6: Received MAJORITY ACKs'")
        print("   - 'Q4 STEP 7: Committing entry N'")
        print("   - 'Q4 EXECUTE: Applied entry N'")
        print("")
        print("3. Screenshot these logs for your report to show Q4 implementation")
        print("="*70 + "\n")


def main():
    print("\n⚠️  PREREQUISITES:")
    print("   1. Docker containers must be running: docker-compose up -d")
    print("   2. Wait 10 seconds for cluster to elect a leader")
    print("   3. This test will submit 3 operations to demonstrate Q4\n")

    response = input("Press Enter to start Q4 demonstration (or 'q' to quit): ")
    if response.lower() == 'q':
        sys.exit(0)

    print("\n🚀 Starting Q4 Demonstration...")
    print("📊 Monitor Docker logs in separate terminals to see Q4 steps:\n")
    print("   Terminal 1: docker logs -f raft_node1")
    print("   Terminal 2: docker logs -f raft_node2")
    print("   Terminal 3: docker logs -f raft_node3")
    print("\n" + "="*70)

    demonstrator = Q4Demonstrator()
    demonstrator.run_demonstration()


if __name__ == "__main__":
    main()
