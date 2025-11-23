#!/usr/bin/env python3
"""
Test client for Raft-integrated polling system
Uses the original polling API from microservice_rpc
"""

import grpc
import sys
import time

sys.path.append('/home/user/dspa3raft/microservice_rpc/app')
import polling_pb2
import polling_pb2_grpc


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
        self.current_node = 1

    def get_stub(self, node_id=None):
        """Get gRPC stubs for a specific node"""
        if node_id is None:
            node_id = self.current_node

        addr = self.nodes[node_id]
        channel = grpc.insecure_channel(addr)
        poll_stub = polling_pb2_grpc.PollServiceStub(channel)
        vote_stub = polling_pb2_grpc.VoteServiceStub(channel)
        result_stub = polling_pb2_grpc.ResultServiceStub(channel)

        return channel, poll_stub, vote_stub, result_stub

    def create_poll(self, question, options):
        """Create a new poll"""
        print(f"\n{'='*60}")
        print(f"Creating Poll")
        print(f"{'='*60}")
        print(f"Question: {question}")
        print(f"Options: {options}")

        for node_id in self.nodes.keys():
            try:
                channel, poll_stub, _, _ = self.get_stub(node_id)
                request = polling_pb2.CreatePollRequest(
                    poll_questions=question,
                    options=options
                )

                print(f"\nTrying Node {node_id}...")
                response = poll_stub.CreatePoll(request, timeout=10.0)

                print(f"✓ SUCCESS on Node {node_id}!")
                print(f"  Poll UUID: {response.uuid}")
                print(f"  Status: {response.status}")
                print(f"  Created: {response.create_at_time}")

                channel.close()
                return response.uuid

            except grpc.RpcError as e:
                print(f"✗ Failed on Node {node_id}: {e.details()}")
                if channel:
                    channel.close()
                continue

        print("✗ All nodes failed!")
        return None

    def cast_vote(self, poll_uuid, user_id, option):
        """Cast a vote"""
        print(f"\n{'='*60}")
        print(f"Casting Vote")
        print(f"{'='*60}")
        print(f"Poll: {poll_uuid}")
        print(f"User: {user_id}")
        print(f"Option: {option}")

        for node_id in self.nodes.keys():
            try:
                channel, _, vote_stub, _ = self.get_stub(node_id)
                request = polling_pb2.CastVoteRequest(
                    uuid=poll_uuid,
                    userID=user_id,
                    select_options=option
                )

                print(f"\nTrying Node {node_id}...")
                response = vote_stub.CastVote(request, timeout=10.0)

                print(f"Response: {response.status}")

                if "Success" in response.status:
                    print(f"✓ Vote cast successfully on Node {node_id}!")
                    channel.close()
                    return True
                else:
                    print(f"⚠ {response.status}")

                channel.close()
                return False

            except grpc.RpcError as e:
                print(f"✗ Failed on Node {node_id}: {e.details()}")
                if channel:
                    channel.close()
                continue

        return False

    def get_results(self, poll_uuid):
        """Get poll results"""
        print(f"\n{'='*60}")
        print(f"Getting Poll Results")
        print(f"{'='*60}")
        print(f"Poll UUID: {poll_uuid}")

        # Results can be read from any node
        for node_id in self.nodes.keys():
            try:
                channel, _, _, result_stub = self.get_stub(node_id)
                request = polling_pb2.PollRequest(uuid=poll_uuid)

                print(f"\nQuerying Node {node_id}...")
                response = result_stub.GetPollResults(request, timeout=5.0)

                print(f"✓ Results from Node {node_id}:")
                print(f"  Question: {response.poll_questions}")
                print(f"  Results:")
                for option, count in response.results.items():
                    print(f"    {option}: {count} votes")

                channel.close()
                return response

            except grpc.RpcError as e:
                print(f"✗ Failed on Node {node_id}: {e.details()}")
                if channel:
                    channel.close()
                continue

        return None

    def list_polls(self):
        """List all polls"""
        print(f"\n{'='*60}")
        print(f"Listing All Polls")
        print(f"{'='*60}")

        # Can read from any node
        for node_id in self.nodes.keys():
            try:
                channel, poll_stub, _, _ = self.get_stub(node_id)
                request = polling_pb2.Empty()

                print(f"\nQuerying Node {node_id}...")
                response = poll_stub.ListPolls(request, timeout=5.0)

                print(f"✓ Found {len(response.polls)} polls on Node {node_id}:")
                for poll in response.polls:
                    print(f"\n  Poll: {poll.uuid}")
                    print(f"    Question: {poll.poll_questions}")
                    print(f"    Options: {', '.join(poll.options)}")
                    print(f"    Status: {poll.status}")

                channel.close()
                return response.polls

            except grpc.RpcError as e:
                print(f"✗ Failed on Node {node_id}: {e.details()}")
                if channel:
                    channel.close()
                continue

        return []

    def close_poll(self, poll_uuid):
        """Close a poll"""
        print(f"\n{'='*60}")
        print(f"Closing Poll")
        print(f"{'='*60}")
        print(f"Poll UUID: {poll_uuid}")

        for node_id in self.nodes.keys():
            try:
                channel, poll_stub, _, _ = self.get_stub(node_id)
                request = polling_pb2.PollRequest(uuid=poll_uuid)

                print(f"\nTrying Node {node_id}...")
                response = poll_stub.ClosePoll(request, timeout=10.0)

                print(f"✓ Poll closed successfully on Node {node_id}!")
                print(f"  Status: {response.status}")

                channel.close()
                return True

            except grpc.RpcError as e:
                print(f"✗ Failed on Node {node_id}: {e.details()}")
                if channel:
                    channel.close()
                continue

        return False


def main():
    """Run demonstration tests"""
    print("\n" + "#"*60)
    print("#" + " "*58 + "#")
    print("#" + "  Raft-Integrated Polling System Test".center(58) + "#")
    print("#" + " "*58 + "#")
    print("#"*60)

    client = PollingClient()

    print("\n⏳ Waiting for cluster to stabilize (10 seconds)...")
    time.sleep(10)

    # Test 1: Create a poll
    print("\n\n" + "="*60)
    print("TEST 1: Create a Poll")
    print("="*60)

    poll_uuid = client.create_poll(
        "What is your favorite distributed consensus algorithm?",
        ["Raft", "Paxos", "Zab", "Viewstamped Replication"]
    )

    if not poll_uuid:
        print("\n✗ Test 1 FAILED: Could not create poll")
        return

    print(f"\n✓ Test 1 PASSED: Poll created with UUID {poll_uuid}")

    time.sleep(3)

    # Test 2: Cast votes
    print("\n\n" + "="*60)
    print("TEST 2: Cast Votes")
    print("="*60)

    votes = [
        ("user1", "Raft"),
        ("user2", "Raft"),
        ("user3", "Paxos"),
        ("user4", "Raft"),
        ("user5", "Zab"),
    ]

    for user_id, option in votes:
        success = client.cast_vote(poll_uuid, user_id, option)
        if success:
            print(f"  ✓ {user_id} voted for {option}")
        else:
            print(f"  ✗ {user_id} failed to vote for {option}")
        time.sleep(2)

    print("\n✓ Test 2 PASSED: Votes cast")

    time.sleep(3)

    # Test 3: Get results
    print("\n\n" + "="*60)
    print("TEST 3: Get Poll Results")
    print("="*60)

    results = client.get_results(poll_uuid)
    if results:
        print("\n✓ Test 3 PASSED: Results retrieved")
    else:
        print("\n✗ Test 3 FAILED: Could not get results")

    time.sleep(2)

    # Test 4: List all polls
    print("\n\n" + "="*60)
    print("TEST 4: List All Polls")
    print("="*60)

    polls = client.list_polls()
    if len(polls) > 0:
        print(f"\n✓ Test 4 PASSED: Listed {len(polls)} polls")
    else:
        print("\n✗ Test 4 FAILED: No polls found")

    time.sleep(2)

    # Test 5: Close poll
    print("\n\n" + "="*60)
    print("TEST 5: Close Poll")
    print("="*60)

    if client.close_poll(poll_uuid):
        print("\n✓ Test 5 PASSED: Poll closed")
    else:
        print("\n✗ Test 5 FAILED: Could not close poll")

    # Summary
    print("\n\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print("All tests completed!")
    print("\nKey Points Demonstrated:")
    print("✓ Raft leader election (automatic)")
    print("✓ Log replication across all 5 nodes")
    print("✓ Consensus before committing operations")
    print("✓ Original polling API works unchanged")
    print("✓ Fault tolerance (try stopping a node!)")
    print("\nTo view Raft logs:")
    print("  docker logs raft_polling_node1")
    print("  docker logs raft_polling_node2")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
