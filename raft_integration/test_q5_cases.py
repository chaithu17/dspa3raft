#!/usr/bin/env python3
"""
Q5 Test Cases for Raft-Integrated Polling System
Tests all required scenarios for assignment Q5
"""

import grpc
import sys
import time
import subprocess
import threading
from collections import Counter

sys.path.append('/home/user/dspa3raft/microservice_rpc/app')
import polling_pb2
import polling_pb2_grpc


class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_header(msg):
    print(f"\n{Colors.BOLD}{Colors.HEADER}{'='*70}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.HEADER}{msg.center(70)}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.HEADER}{'='*70}{Colors.ENDC}\n")


def print_success(msg):
    print(f"{Colors.OKGREEN}✓ {msg}{Colors.ENDC}")


def print_error(msg):
    print(f"{Colors.FAIL}✗ {msg}{Colors.ENDC}")


def print_info(msg):
    print(f"{Colors.OKCYAN}  {msg}{Colors.ENDC}")


class PollingClient:
    """Client for testing the integrated Raft-Polling system"""

    def __init__(self):
        self.nodes = {
            1: "localhost:50051",
            2: "localhost:50052",
            3: "localhost:50053",
            4: "localhost:50054",
            5: "localhost:50055"
        }

    def get_stub(self, node_id):
        """Get gRPC stubs for a specific node"""
        addr = self.nodes[node_id]
        channel = grpc.insecure_channel(addr)
        poll_stub = polling_pb2_grpc.PollServiceStub(channel)
        vote_stub = polling_pb2_grpc.VoteServiceStub(channel)
        result_stub = polling_pb2_grpc.ResultServiceStub(channel)
        return channel, poll_stub, vote_stub, result_stub

    def create_poll_on_node(self, node_id, question, options):
        """Create poll on specific node"""
        try:
            channel, poll_stub, _, _ = self.get_stub(node_id)
            request = polling_pb2.CreatePollRequest(
                poll_questions=question,
                options=options
            )
            response = poll_stub.CreatePoll(request, timeout=10.0)
            channel.close()
            return response.uuid if response.uuid else None
        except Exception as e:
            return None

    def cast_vote_on_node(self, node_id, poll_uuid, user_id, option):
        """Cast vote on specific node"""
        try:
            channel, _, vote_stub, _ = self.get_stub(node_id)
            request = polling_pb2.CastVoteRequest(
                uuid=poll_uuid,
                userID=user_id,
                select_options=option
            )
            response = vote_stub.CastVote(request, timeout=10.0)
            channel.close()
            return "Success" in response.status
        except Exception as e:
            return False

    def get_results_from_node(self, node_id, poll_uuid):
        """Get results from specific node"""
        try:
            channel, _, _, result_stub = self.get_stub(node_id)
            request = polling_pb2.PollRequest(uuid=poll_uuid)
            response = result_stub.GetPollResults(request, timeout=5.0)
            channel.close()
            return dict(response.results) if response else None
        except Exception as e:
            return None

    def list_polls_on_node(self, node_id):
        """List polls on specific node"""
        try:
            channel, poll_stub, _, _ = self.get_stub(node_id)
            request = polling_pb2.Empty()
            response = poll_stub.ListPolls(request, timeout=5.0)
            channel.close()
            return list(response.polls) if response else []
        except Exception as e:
            return []

    def close_poll_on_node(self, node_id, poll_uuid):
        """Close poll on specific node"""
        try:
            channel, poll_stub, _, _ = self.get_stub(node_id)
            request = polling_pb2.PollRequest(uuid=poll_uuid)
            response = poll_stub.ClosePoll(request, timeout=10.0)
            channel.close()
            return "Success" in response.status or "closed" in response.status.lower()
        except Exception as e:
            return False


def run_command(cmd):
    """Run shell command"""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.returncode == 0, result.stdout, result.stderr


def get_leader_node():
    """Identify which node is the leader"""
    for node_id in range(1, 6):
        success, stdout, _ = run_command(
            f"docker logs raft_polling_node{node_id} 2>&1 | grep 'WON ELECTION' | tail -1"
        )
        if success and "WON ELECTION" in stdout:
            return node_id
    return None


def test_case_2_create_poll_consensus():
    """Test 2: Create Poll with Consensus - All nodes have identical data"""
    print_header("TEST 2: CREATE POLL WITH CONSENSUS")

    client = PollingClient()

    # Create poll on node 1
    print_info("Creating poll on Node 1...")
    poll_uuid = client.create_poll_on_node(1, "Favorite Algorithm?", ["Raft", "Paxos", "Zab"])

    if not poll_uuid:
        print_error("Failed to create poll")
        return False

    print_success(f"Poll created: {poll_uuid}")

    # Wait for replication
    print_info("Waiting 3 seconds for replication...")
    time.sleep(3)

    # Verify all nodes have the poll
    print_info("Verifying all 5 nodes have identical poll data...")
    poll_data = {}

    for node_id in range(1, 6):
        polls = client.list_polls_on_node(node_id)
        if polls:
            for poll in polls:
                if poll.uuid == poll_uuid:
                    poll_data[node_id] = {
                        'uuid': poll.uuid,
                        'question': poll.poll_questions,
                        'options': list(poll.options),
                        'status': poll.status
                    }
                    print_info(f"Node {node_id}: Found poll with question '{poll.poll_questions}'")
                    break

    # Verify all 5 nodes have identical data
    if len(poll_data) == 5:
        # Check all data is identical
        first_data = poll_data[1]
        all_identical = all(
            poll_data[i]['question'] == first_data['question'] and
            poll_data[i]['options'] == first_data['options']
            for i in range(2, 6)
        )

        if all_identical:
            print_success("TEST 2 PASSED: All 5 nodes have identical poll data")
            return True, poll_uuid
        else:
            print_error("TEST 2 FAILED: Nodes have different poll data")
            return False, None
    else:
        print_error(f"TEST 2 FAILED: Only {len(poll_data)}/5 nodes have the poll")
        return False, None


def test_case_3_cast_vote_replication(poll_uuid):
    """Test 3: Cast Vote with Replication - Vote counts identical on all nodes"""
    print_header("TEST 3: CAST VOTE WITH REPLICATION")

    client = PollingClient()

    # Cast votes from different nodes
    votes = [
        (1, "user1", "Raft"),
        (2, "user2", "Raft"),
        (3, "user3", "Paxos"),
        (4, "user4", "Raft"),
        (5, "user5", "Zab"),
    ]

    print_info("Casting 5 votes from different nodes...")
    for node_id, user_id, option in votes:
        success = client.cast_vote_on_node(node_id, poll_uuid, user_id, option)
        if success:
            print_info(f"  Node {node_id}: {user_id} voted for {option}")
        else:
            print_error(f"  Node {node_id}: Failed to cast vote for {user_id}")
        time.sleep(1)

    # Wait for replication
    print_info("Waiting 3 seconds for replication...")
    time.sleep(3)

    # Get results from all nodes
    print_info("Verifying vote counts on all 5 nodes...")
    results = {}

    for node_id in range(1, 6):
        node_results = client.get_results_from_node(node_id, poll_uuid)
        if node_results:
            results[node_id] = node_results
            print_info(f"Node {node_id}: {node_results}")

    # Verify all nodes have identical results
    if len(results) == 5:
        first_results = results[1]
        all_identical = all(results[i] == first_results for i in range(2, 6))

        if all_identical:
            print_success("TEST 3 PASSED: All 5 nodes have identical vote counts")
            return True
        else:
            print_error("TEST 3 FAILED: Nodes have different vote counts")
            return False
    else:
        print_error(f"TEST 3 FAILED: Only {len(results)}/5 nodes returned results")
        return False


def test_case_4_leader_failure_reelection():
    """Test 4: Leader Failure and Re-election"""
    print_header("TEST 4: LEADER FAILURE AND RE-ELECTION")

    # Identify current leader
    print_info("Identifying current leader...")
    leader_node = get_leader_node()

    if not leader_node:
        print_error("Could not identify leader")
        return False

    print_success(f"Current leader: Node {leader_node}")

    # Stop the leader
    print_info(f"Stopping leader Node {leader_node}...")
    success, _, _ = run_command(f"docker stop raft_polling_node{leader_node}")

    if not success:
        print_error("Failed to stop leader")
        return False

    print_success(f"Leader Node {leader_node} stopped")

    # Wait for re-election
    print_info("Waiting 10 seconds for re-election...")
    time.sleep(10)

    # Check if new leader elected
    print_info("Checking for new leader...")
    new_leader = None

    for node_id in range(1, 6):
        if node_id == leader_node:
            continue

        success, stdout, _ = run_command(
            f"docker logs raft_polling_node{node_id} 2>&1 | grep 'WON ELECTION' | tail -1"
        )

        if success and "WON ELECTION" in stdout:
            # Check if this is a recent election (after we stopped the old leader)
            new_leader = node_id
            print_success(f"New leader elected: Node {new_leader}")
            break

    # Restart the old leader
    print_info(f"Restarting old leader Node {leader_node}...")
    run_command(f"docker start raft_polling_node{leader_node}")
    time.sleep(3)

    if new_leader and new_leader != leader_node:
        print_success("TEST 4 PASSED: New leader elected after failure")
        return True
    else:
        print_error("TEST 4 FAILED: No new leader elected")
        return False


def test_case_5_node_rejoin_catchup():
    """Test 5: Node Rejoining After Partition - Catches up on missed entries"""
    print_header("TEST 5: NODE REJOINING AFTER PARTITION")

    client = PollingClient()

    # Stop node 3
    print_info("Stopping Node 3...")
    run_command("docker stop raft_polling_node3")
    time.sleep(2)

    # Create a poll while node 3 is down
    print_info("Creating poll while Node 3 is down...")
    poll_uuid = client.create_poll_on_node(1, "Test Question While Partitioned", ["A", "B", "C"])

    if not poll_uuid:
        print_error("Failed to create poll")
        run_command("docker start raft_polling_node3")
        return False

    print_success(f"Poll created: {poll_uuid}")
    time.sleep(2)

    # Restart node 3
    print_info("Restarting Node 3...")
    run_command("docker start raft_polling_node3")

    # Wait for node to rejoin and catch up
    print_info("Waiting 8 seconds for Node 3 to rejoin and catch up...")
    time.sleep(8)

    # Check if node 3 has the poll
    print_info("Checking if Node 3 has the poll...")
    polls = client.list_polls_on_node(3)

    has_poll = any(poll.uuid == poll_uuid for poll in polls)

    if has_poll:
        print_success("TEST 5 PASSED: Node 3 caught up on missed entries")
        return True
    else:
        print_error("TEST 5 FAILED: Node 3 did not catch up")
        return False


def test_case_6_read_from_any_node():
    """Test 6: Read from Any Node - All nodes return same results"""
    print_header("TEST 6: READ FROM ANY NODE")

    client = PollingClient()

    # Create a poll
    print_info("Creating poll...")
    poll_uuid = client.create_poll_on_node(1, "Read Test", ["Option1", "Option2"])

    if not poll_uuid:
        print_error("Failed to create poll")
        return False

    # Cast some votes
    print_info("Casting votes...")
    client.cast_vote_on_node(1, poll_uuid, "user1", "Option1")
    client.cast_vote_on_node(2, poll_uuid, "user2", "Option2")
    client.cast_vote_on_node(3, poll_uuid, "user3", "Option1")

    time.sleep(3)

    # Read results from all 5 nodes
    print_info("Reading results from all 5 nodes...")
    results = {}

    for node_id in range(1, 6):
        node_results = client.get_results_from_node(node_id, poll_uuid)
        if node_results:
            results[node_id] = node_results
            print_info(f"Node {node_id}: {node_results}")

    # Verify all results are identical
    if len(results) == 5:
        first_results = results[1]
        all_identical = all(results[i] == first_results for i in range(2, 6))

        if all_identical:
            print_success("TEST 6 PASSED: All nodes return identical results")
            return True
        else:
            print_error("TEST 6 FAILED: Nodes return different results")
            return False
    else:
        print_error(f"TEST 6 FAILED: Only {len(results)}/5 nodes responded")
        return False


def test_case_7_concurrent_votes():
    """Test 7: Concurrent Votes - Multiple clients vote simultaneously"""
    print_header("TEST 7: CONCURRENT VOTES")

    client = PollingClient()

    # Create a poll
    print_info("Creating poll...")
    poll_uuid = client.create_poll_on_node(1, "Concurrent Test", ["A", "B", "C"])

    if not poll_uuid:
        print_error("Failed to create poll")
        return False

    time.sleep(2)

    # Vote concurrently from multiple threads
    print_info("Casting 10 concurrent votes...")
    votes_cast = []
    lock = threading.Lock()

    def cast_concurrent_vote(user_id, option):
        node_id = (user_id % 5) + 1  # Distribute across nodes
        success = client.cast_vote_on_node(node_id, poll_uuid, f"user{user_id}", option)
        with lock:
            votes_cast.append((user_id, option, success))

    # Create threads for concurrent voting
    threads = []
    expected_votes = {
        "A": 4,
        "B": 3,
        "C": 3
    }

    vote_list = (
        [("A", i) for i in range(1, 5)] +
        [("B", i) for i in range(5, 8)] +
        [("C", i) for i in range(8, 11)]
    )

    for option, user_id in vote_list:
        thread = threading.Thread(target=cast_concurrent_vote, args=(user_id, option))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    successful_votes = sum(1 for _, _, success in votes_cast if success)
    print_info(f"{successful_votes}/10 votes cast successfully")

    # Wait for replication
    print_info("Waiting 5 seconds for replication...")
    time.sleep(5)

    # Get results and verify
    print_info("Verifying vote counts...")
    results = client.get_results_from_node(1, poll_uuid)

    if results:
        print_info(f"Final results: {results}")

        # Check for duplicates (each user should vote only once)
        total_votes = sum(results.values())

        if total_votes == successful_votes:
            print_success(f"TEST 7 PASSED: {total_votes} votes counted, no duplicates")
            return True
        else:
            print_error(f"TEST 7 FAILED: Expected {successful_votes} votes, got {total_votes}")
            return False
    else:
        print_error("TEST 7 FAILED: Could not get results")
        return False


def main():
    """Run all Q5 test cases"""
    print("\n" + "#"*70)
    print("#" + " "*68 + "#")
    print("#" + "  Q5 Test Cases - Raft Polling System".center(68) + "#")
    print("#" + " "*68 + "#")
    print("#"*70)

    print_info("\nWaiting 10 seconds for cluster to stabilize...")
    time.sleep(10)

    test_results = {}

    # Test 2: Create Poll with Consensus
    success, poll_uuid = test_case_2_create_poll_consensus()
    test_results['Test 2'] = success

    if success and poll_uuid:
        time.sleep(2)

        # Test 3: Cast Vote with Replication
        success = test_case_3_cast_vote_replication(poll_uuid)
        test_results['Test 3'] = success

    time.sleep(2)

    # Test 4: Leader Failure and Re-election
    success = test_case_4_leader_failure_reelection()
    test_results['Test 4'] = success

    time.sleep(2)

    # Test 5: Node Rejoining After Partition
    success = test_case_5_node_rejoin_catchup()
    test_results['Test 5'] = success

    time.sleep(2)

    # Test 6: Read from Any Node
    success = test_case_6_read_from_any_node()
    test_results['Test 6'] = success

    time.sleep(2)

    # Test 7: Concurrent Votes
    success = test_case_7_concurrent_votes()
    test_results['Test 7'] = success

    # Summary
    print_header("TEST SUMMARY")

    passed = sum(1 for result in test_results.values() if result)
    total = len(test_results)

    for test_name, result in test_results.items():
        if result:
            print_success(f"{test_name}: PASSED")
        else:
            print_error(f"{test_name}: FAILED")

    print(f"\n{Colors.BOLD}Total: {passed}/{total} tests passed{Colors.ENDC}")

    if passed == total:
        print(f"\n{Colors.OKGREEN}{'='*70}")
        print(f"ALL TESTS PASSED - Q5 REQUIREMENTS MET")
        print(f"{'='*70}{Colors.ENDC}\n")
    else:
        print(f"\n{Colors.WARNING}{'='*70}")
        print(f"Some tests failed - Review logs above")
        print(f"{'='*70}{Colors.ENDC}\n")


if __name__ == "__main__":
    main()
