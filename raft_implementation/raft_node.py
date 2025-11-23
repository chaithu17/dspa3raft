#!/usr/bin/env python3
"""
Raft Consensus Algorithm Implementation
Implements leader election (Q3) and log replication (Q4)
"""

import grpc
from concurrent import futures
import time
import random
import threading
import json
import sys
from enum import Enum
from typing import List, Dict, Optional

import raft_pb2
import raft_pb2_grpc


class NodeState(Enum):
    """Raft node states"""
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


class RaftNode(raft_pb2_grpc.RaftNodeServicer):
    """
    Raft Node implementation with leader election and log replication
    """

    def __init__(self, node_id: int, peers: Dict[int, str]):
        """
        Initialize a Raft node

        Args:
            node_id: Unique identifier for this node
            peers: Dictionary mapping node_id -> "host:port" for all nodes in cluster
        """
        self.node_id = node_id
        self.peers = peers  # {node_id: "host:port"}

        # Persistent state on all servers
        self.current_term = 0
        self.voted_for = None  # candidateId that received vote in current term
        self.log: List[raft_pb2.LogEntry] = []  # log entries

        # Volatile state on all servers
        self.commit_index = 0  # index of highest log entry known to be committed
        self.last_applied = 0  # index of highest log entry applied to state machine
        self.state = NodeState.FOLLOWER

        # Volatile state on leaders (reinitialized after election)
        self.next_index: Dict[int, int] = {}  # for each server, index of next log entry to send
        self.match_index: Dict[int, int] = {}  # for each server, index of highest log entry known to be replicated

        # Timing
        self.heartbeat_timeout = 1.0  # 1 second
        self.election_timeout = random.uniform(1.5, 3.0)  # random [1.5s, 3s]
        self.last_heartbeat = time.time()

        # Thread control
        self.running = True
        self.election_thread = None
        self.heartbeat_thread = None

        # Leader tracking
        self.current_leader = None

        # State machine (simple key-value store for polling operations)
        self.state_machine: Dict[str, any] = {
            "polls": {},  # poll_id -> {question, options, votes}
            "operation_count": 0
        }

        # Locks
        self.lock = threading.RLock()

        print(f"[Node {self.node_id}] Initialized as {self.state.value}")
        print(f"[Node {self.node_id}] Election timeout: {self.election_timeout:.2f}s")
        print(f"[Node {self.node_id}] Peers: {peers}")

    def start(self):
        """Start the Raft node background threads"""
        self.election_thread = threading.Thread(target=self._election_timer, daemon=True)
        self.election_thread.start()
        print(f"[Node {self.node_id}] Started election timer thread")

    def _election_timer(self):
        """Background thread that monitors election timeout"""
        while self.running:
            time.sleep(0.1)  # Check every 100ms

            with self.lock:
                # Only followers and candidates should timeout
                if self.state == NodeState.LEADER:
                    continue

                elapsed = time.time() - self.last_heartbeat

                if elapsed > self.election_timeout:
                    print(f"\n[Node {self.node_id}] Election timeout ({self.election_timeout:.2f}s) reached!")
                    self._start_election()

    def _start_election(self):
        """Initiate leader election (Q3)"""
        with self.lock:
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.last_heartbeat = time.time()
            self.election_timeout = random.uniform(1.5, 3.0)  # New random timeout

            current_term = self.current_term

            print(f"\n{'='*60}")
            print(f"[Node {self.node_id}] Starting election for term {current_term}")
            print(f"[Node {self.node_id}] State: {NodeState.FOLLOWER.value} -> {NodeState.CANDIDATE.value}")
            print(f"{'='*60}")

        # Vote for self
        votes_received = 1
        votes_needed = (len(self.peers) + 1) // 2 + 1  # Majority

        print(f"[Node {self.node_id}] Voted for self. Votes: 1/{len(self.peers)} (need {votes_needed})")

        # Request votes from all peers
        for peer_id, peer_addr in self.peers.items():
            if peer_id == self.node_id:
                continue

            try:
                # Get last log info
                last_log_index = len(self.log)
                last_log_term = self.log[-1].term if self.log else 0

                # Send RequestVote RPC
                with grpc.insecure_channel(peer_addr) as channel:
                    stub = raft_pb2_grpc.RaftNodeStub(channel)

                    request = raft_pb2.RequestVoteRequest(
                        term=current_term,
                        candidate_id=self.node_id,
                        last_log_index=last_log_index,
                        last_log_term=last_log_term
                    )

                    print(f"[Node {self.node_id}] sends RPC RequestVote to Node {peer_id}")

                    response = stub.RequestVote(request, timeout=0.5)

                    with self.lock:
                        # Check if we're still a candidate and in the same term
                        if self.state != NodeState.CANDIDATE or self.current_term != current_term:
                            return

                        # If we discover a higher term, step down
                        if response.term > self.current_term:
                            print(f"[Node {self.node_id}] Discovered higher term {response.term}, stepping down")
                            self._step_down(response.term)
                            return

                        # Count the vote
                        if response.vote_granted:
                            votes_received += 1
                            print(f"[Node {self.node_id}] Received vote from Node {peer_id}. Votes: {votes_received}/{len(self.peers)}")

                            # Check if we won the election
                            if votes_received >= votes_needed:
                                self._become_leader()
                                return
                        else:
                            print(f"[Node {self.node_id}] Vote denied by Node {peer_id}")

            except Exception as e:
                print(f"[Node {self.node_id}] Failed to get vote from Node {peer_id}: {e}")

    def _become_leader(self):
        """Transition to leader state"""
        with self.lock:
            if self.state != NodeState.CANDIDATE:
                return

            print(f"\n{'='*60}")
            print(f"[Node {self.node_id}] WON ELECTION for term {self.current_term}!")
            print(f"[Node {self.node_id}] State: {NodeState.CANDIDATE.value} -> {NodeState.LEADER.value}")
            print(f"{'='*60}\n")

            self.state = NodeState.LEADER
            self.current_leader = self.node_id

            # Initialize leader state
            for peer_id in self.peers:
                if peer_id != self.node_id:
                    self.next_index[peer_id] = len(self.log) + 1
                    self.match_index[peer_id] = 0

            # Start sending heartbeats
            if self.heartbeat_thread is None or not self.heartbeat_thread.is_alive():
                self.heartbeat_thread = threading.Thread(target=self._send_heartbeats, daemon=True)
                self.heartbeat_thread.start()

    def _send_heartbeats(self):
        """Leader sends periodic heartbeats (Q3) and replicates log (Q4)"""
        while self.running:
            with self.lock:
                if self.state != NodeState.LEADER:
                    return

                current_term = self.current_term

            # Send AppendEntries (heartbeat or log replication) to all followers
            for peer_id, peer_addr in self.peers.items():
                if peer_id == self.node_id:
                    continue

                try:
                    with self.lock:
                        if self.state != NodeState.LEADER:
                            return

                        # Determine which entries to send
                        next_idx = self.next_index.get(peer_id, 1)
                        prev_log_index = next_idx - 1
                        prev_log_term = self.log[prev_log_index - 1].term if prev_log_index > 0 else 0

                        # Get entries to send (empty for heartbeat)
                        entries = []
                        if len(self.log) >= next_idx:
                            entries = self.log[next_idx - 1:]

                    # Send AppendEntries RPC
                    with grpc.insecure_channel(peer_addr) as channel:
                        stub = raft_pb2_grpc.RaftNodeStub(channel)

                        request = raft_pb2.AppendEntriesRequest(
                            term=current_term,
                            leader_id=self.node_id,
                            prev_log_index=prev_log_index,
                            prev_log_term=prev_log_term,
                            entries=entries,
                            leader_commit=self.commit_index
                        )

                        # Q4 STEP 3 LOGGING: Show what we're sending
                        if entries:
                            print(f"[Node {self.node_id}] Q4 STEP 3: Sending {len(entries)} log entries to Node {peer_id}")
                            print(f"[Node {self.node_id}]            Entries: {[f'idx={e.index}' for e in entries]}")
                            print(f"[Node {self.node_id}]            With commit index c={self.commit_index}")

                        msg_type = "AppendEntries (heartbeat)" if not entries else f"AppendEntries ({len(entries)} entries)"
                        print(f"[Node {self.node_id}] sends RPC {msg_type} to Node {peer_id}")

                        response = stub.AppendEntries(request, timeout=0.5)

                        with self.lock:
                            if self.state != NodeState.LEADER:
                                return

                            # Update term if higher term discovered
                            if response.term > self.current_term:
                                print(f"[Node {self.node_id}] Discovered higher term, stepping down from leader")
                                self._step_down(response.term)
                                return

                            # Update follower's indices (Q4: Track ACKs)
                            if response.success:
                                if entries:
                                    self.match_index[peer_id] = prev_log_index + len(entries)
                                    self.next_index[peer_id] = self.match_index[peer_id] + 1
                                    print(f"[Node {self.node_id}] Q4: Received ACK from Node {peer_id} (replicated up to {self.match_index[peer_id]})")
                            else:
                                # Decrement next_index and retry
                                self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)
                                print(f"[Node {self.node_id}] Node {peer_id} rejected, decrementing next_index to {self.next_index[peer_id]}")

                except Exception as e:
                    print(f"[Node {self.node_id}] Failed to send heartbeat to Node {peer_id}: {e}")

            # Check if we can commit any entries (Q4)
            self._update_commit_index()

            # Apply committed entries
            self._apply_committed_entries()

            # Sleep for heartbeat interval
            time.sleep(self.heartbeat_timeout)

    def _update_commit_index(self):
        """Update commit index based on majority replication (Q4)"""
        with self.lock:
            if self.state != NodeState.LEADER:
                return

            # Find highest index replicated on majority
            for n in range(len(self.log), self.commit_index, -1):
                if n == 0:
                    break

                # Count how many nodes have replicated this entry (Q4 STEP 6)
                replicated_count = 1  # Leader has it
                for peer_id in self.peers:
                    if peer_id != self.node_id and self.match_index.get(peer_id, 0) >= n:
                        replicated_count += 1

                # If majority has replicated and it's from current term
                majority = (len(self.peers) + 1) // 2 + 1
                if replicated_count >= majority and self.log[n-1].term == self.current_term:
                    if n > self.commit_index:
                        print(f"[Node {self.node_id}] Q4 STEP 6: Received MAJORITY ACKs ({replicated_count}/{len(self.peers)+1} nodes)")
                        print(f"[Node {self.node_id}] Q4 STEP 7: Committing entry {n} (was pending, now committed)")
                        print(f"[Node {self.node_id}]            Incrementing c: {self.commit_index} → {n}")
                        self.commit_index = n
                    break

    def _apply_committed_entries(self):
        """Apply committed log entries to state machine"""
        with self.lock:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log[self.last_applied - 1]

                # Apply operation to state machine
                try:
                    data = json.loads(entry.data)
                    result = self._execute_operation(entry.operation, data)
                    print(f"[Node {self.node_id}] Q4 EXECUTE: Applied entry {self.last_applied}: {entry.operation}")
                    print(f"[Node {self.node_id}]             Data: {entry.data[:80]}")
                except Exception as e:
                    print(f"[Node {self.node_id}] Error applying entry {self.last_applied}: {e}")

    def _execute_operation(self, operation: str, data: dict) -> any:
        """Execute operation on state machine"""
        if operation == "CREATE_POLL":
            poll_id = f"poll_{len(self.state_machine['polls']) + 1}"
            self.state_machine["polls"][poll_id] = {
                "question": data.get("question"),
                "options": data.get("options", []),
                "votes": {opt: 0 for opt in data.get("options", [])}
            }
            return poll_id

        elif operation == "VOTE":
            poll_id = data.get("poll_id")
            option = data.get("option")
            if poll_id in self.state_machine["polls"]:
                if option in self.state_machine["polls"][poll_id]["votes"]:
                    self.state_machine["polls"][poll_id]["votes"][option] += 1
                    return True
            return False

        elif operation == "GET_RESULTS":
            poll_id = data.get("poll_id")
            if poll_id in self.state_machine["polls"]:
                return self.state_machine["polls"][poll_id]
            return None

        self.state_machine["operation_count"] += 1
        return True

    def _step_down(self, new_term: int):
        """Step down to follower state"""
        with self.lock:
            old_state = self.state
            self.state = NodeState.FOLLOWER
            self.current_term = new_term
            self.voted_for = None
            self.last_heartbeat = time.time()
            self.election_timeout = random.uniform(1.5, 3.0)

            if old_state != NodeState.FOLLOWER:
                print(f"[Node {self.node_id}] State: {old_state.value} -> {NodeState.FOLLOWER.value} (term {new_term})")

    # gRPC Service Methods

    def RequestVote(self, request, context):
        """Handle RequestVote RPC (Q3)"""
        print(f"[Node {self.node_id}] runs RPC RequestVote called by Node {request.candidate_id}")

        with self.lock:
            # Update term if needed
            if request.term > self.current_term:
                self._step_down(request.term)

            vote_granted = False

            # Grant vote if:
            # 1. Candidate's term >= our term
            # 2. We haven't voted for anyone else this term
            # 3. Candidate's log is at least as up-to-date as ours
            if request.term >= self.current_term:
                if self.voted_for is None or self.voted_for == request.candidate_id:
                    # Check if candidate's log is up-to-date
                    our_last_log_index = len(self.log)
                    our_last_log_term = self.log[-1].term if self.log else 0

                    log_ok = (request.last_log_term > our_last_log_term or
                             (request.last_log_term == our_last_log_term and
                              request.last_log_index >= our_last_log_index))

                    if log_ok:
                        vote_granted = True
                        self.voted_for = request.candidate_id
                        self.last_heartbeat = time.time()  # Reset election timer
                        print(f"[Node {self.node_id}] Granted vote to Node {request.candidate_id} for term {request.term}")
                    else:
                        print(f"[Node {self.node_id}] Denied vote to Node {request.candidate_id} (log not up-to-date)")
                else:
                    print(f"[Node {self.node_id}] Denied vote to Node {request.candidate_id} (already voted for Node {self.voted_for})")
            else:
                print(f"[Node {self.node_id}] Denied vote to Node {request.candidate_id} (term {request.term} < {self.current_term})")

            return raft_pb2.RequestVoteResponse(
                term=self.current_term,
                vote_granted=vote_granted
            )

    def AppendEntries(self, request, context):
        """Handle AppendEntries RPC - heartbeat and log replication (Q3, Q4)"""
        msg_type = "heartbeat" if not request.entries else f"{len(request.entries)} entries"
        print(f"[Node {self.node_id}] runs RPC AppendEntries ({msg_type}) called by Node {request.leader_id}")

        with self.lock:
            # Update term if needed
            if request.term > self.current_term:
                self._step_down(request.term)

            success = False

            if request.term >= self.current_term:
                # Valid leader - reset election timer
                self.last_heartbeat = time.time()
                self.current_leader = request.leader_id

                if self.state != NodeState.FOLLOWER:
                    self._step_down(request.term)

                # Check log consistency
                if request.prev_log_index == 0 or \
                   (request.prev_log_index <= len(self.log) and
                    (request.prev_log_index == 0 or self.log[request.prev_log_index - 1].term == request.prev_log_term)):

                    success = True

                    # Append new entries (Q4 STEP 4: Follower copies log)
                    if request.entries:
                        # Delete conflicting entries and append new ones
                        self.log = self.log[:request.prev_log_index]
                        self.log.extend(request.entries)
                        print(f"[Node {self.node_id}] Q4 STEP 4: Follower copied {len(request.entries)} entries to log")
                        print(f"[Node {self.node_id}]            Log size now: {len(self.log)}")
                        for entry in request.entries:
                            print(f"[Node {self.node_id}]            - Entry {entry.index}: {entry.operation} (term {entry.term})")

                    # Update commit index (Q4 STEP 5)
                    if request.leader_commit > self.commit_index:
                        old_commit = self.commit_index
                        self.commit_index = min(request.leader_commit, len(self.log))
                        print(f"[Node {self.node_id}] Q4 STEP 5: Updated commit index c: {old_commit} → {self.commit_index}")
                        print(f"[Node {self.node_id}]            Will execute operations up to index {self.commit_index}")

                        # Apply committed entries
                        self._apply_committed_entries()
                else:
                    print(f"[Node {self.node_id}] Log inconsistency at index {request.prev_log_index}")

            return raft_pb2.AppendEntriesResponse(
                term=self.current_term,
                success=success,
                match_index=len(self.log) if success else 0
            )

    def ClientRequest(self, request, context):
        """Handle client requests (Q4)"""
        print(f"[Node {self.node_id}] runs RPC ClientRequest called by client")

        with self.lock:
            # If not leader, redirect to leader
            if self.state != NodeState.LEADER:
                leader_id = self.current_leader if self.current_leader is not None else -1
                print(f"[Node {self.node_id}] ❌ NOT LEADER - Redirecting client to Node {leader_id}")
                return raft_pb2.ClientRequestResponse(
                    success=False,
                    message=f"Not the leader. Current leader: Node {leader_id}",
                    result="",
                    leader_id=leader_id
                )

            print(f"[Node {self.node_id}] ✓ I AM LEADER - Processing client request")
            print(f"[Node {self.node_id}] Q4 STEP 1: Leader received request for operation '{request.operation}'")

            # Leader: append to log (Q4 STEP 2)
            new_entry = raft_pb2.LogEntry(
                term=self.current_term,
                index=len(self.log) + 1,
                operation=request.operation,
                data=request.data
            )

            self.log.append(new_entry)
            target_index = len(self.log)

            print(f"[Node {self.node_id}] Q4 STEP 2: Appended <{request.operation}, t={self.current_term}, k+1={target_index}> to log")
            print(f"[Node {self.node_id}]           Log size: {len(self.log)}, Commit index c={self.commit_index}")
            print(f"[Node {self.node_id}]           Entry details: index={new_entry.index}, term={new_entry.term}, op={new_entry.operation}")

        # Q4 STEP 3 happens in _send_heartbeats
        print(f"[Node {self.node_id}] Q4 STEP 3: Will send log to all followers on next heartbeat...")

        # Wait for replication and commitment
        target_index = len(self.log)

        # Release lock while waiting
        max_wait = 5.0  # 5 seconds timeout
        start_time = time.time()

        while time.time() - start_time < max_wait:
            with self.lock:
                if self.commit_index >= target_index:
                    # Entry committed!
                    print(f"[Node {self.node_id}] Q4 FINAL: Operation committed at index {target_index}")
                    print(f"[Node {self.node_id}]          Commit index c incremented: {target_index-1} → {self.commit_index}")
                    result_data = json.dumps({"status": "committed", "index": target_index})
                    return raft_pb2.ClientRequestResponse(
                        success=True,
                        message=f"Operation committed at index {target_index}",
                        result=result_data,
                        leader_id=self.node_id
                    )
            time.sleep(0.1)

        # Timeout
        return raft_pb2.ClientRequestResponse(
            success=False,
            message="Timeout waiting for commit",
            result="",
            leader_id=self.node_id
        )


def serve(node_id: int, port: int, peers: Dict[int, str]):
    """Start the Raft node gRPC server"""
    node = RaftNode(node_id, peers)
    node.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftNodeServicer_to_server(node, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()

    print(f"\n[Node {node_id}] Server started on port {port}")
    print(f"[Node {node_id}] Waiting for heartbeat or starting election...\n")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        node.running = False
        server.stop(0)
        print(f"\n[Node {node_id}] Shutting down...")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python raft_node.py <node_id>")
        sys.exit(1)

    node_id = int(sys.argv[1])

    # Define cluster configuration (5 nodes)
    # In Docker, use service names. For local testing, use localhost
    peers = {
        1: "node1:50051",
        2: "node2:50052",
        3: "node3:50053",
        4: "node4:50054",
        5: "node5:50055"
    }

    port = 50050 + node_id

    serve(node_id, port, peers)
