#!/usr/bin/env python3
"""
Raft-Integrated Polling System
Integrates Raft consensus with the microservice_rpc polling system
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
import uuid as uuid_lib

# Import Raft proto
sys.path.append('/home/user/dspa3raft/raft_implementation')
import raft_pb2
import raft_pb2_grpc

# Import Polling proto from their system
sys.path.append('/home/user/dspa3raft/microservice_rpc/app')
import polling_pb2
import polling_pb2_grpc


class NodeState(Enum):
    """Raft node states"""
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


class RaftPollingNode(raft_pb2_grpc.RaftNodeServicer,
                      polling_pb2_grpc.PollServiceServicer,
                      polling_pb2_grpc.VoteServiceServicer,
                      polling_pb2_grpc.ResultServiceServicer):
    """
    Integrated Raft + Polling System
    Combines Raft consensus with the voting microservice
    """

    def __init__(self, node_id: int, peers: Dict[int, str]):
        """
        Initialize integrated node

        Args:
            node_id: Unique identifier for this node
            peers: Dictionary mapping node_id -> "host:port"
        """
        self.node_id = node_id
        self.peers = peers

        # ============= RAFT STATE =============
        # Persistent state
        self.current_term = 0
        self.voted_for = None
        self.log: List[raft_pb2.LogEntry] = []

        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        self.state = NodeState.FOLLOWER

        # Leader state
        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}

        # Timing
        self.heartbeat_timeout = 1.0
        self.election_timeout = random.uniform(1.5, 3.0)
        self.last_heartbeat = time.time()

        # Thread control
        self.running = True
        self.election_thread = None
        self.heartbeat_thread = None
        self.current_leader = None

        # ============= VOTING SYSTEM STATE =============
        # Replaces PostgreSQL database with in-memory state
        self.state_machine: Dict[str, any] = {
            "polls": {},      # uuid -> {poll_questions, options, status, create_at_time, votes}
            "user_votes": {}  # (userID, poll_uuid) -> selected_option (for duplicate detection)
        }

        # Locks
        self.lock = threading.RLock()

        print(f"[Node {self.node_id}] Initialized Raft-Polling Node as {self.state.value}")
        print(f"[Node {self.node_id}] Election timeout: {self.election_timeout:.2f}s")
        print(f"[Node {self.node_id}] Peers: {peers}")

    def start(self):
        """Start the Raft node background threads"""
        self.election_thread = threading.Thread(target=self._election_timer, daemon=True)
        self.election_thread.start()
        print(f"[Node {self.node_id}] Started election timer thread")

    # ==================== RAFT CORE LOGIC ====================
    # (Copied from raft_node.py with Q4 logging)

    def _election_timer(self):
        """Background thread that monitors election timeout"""
        while self.running:
            time.sleep(0.1)
            with self.lock:
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
            self.election_timeout = random.uniform(1.5, 3.0)
            current_term = self.current_term

            print(f"\n{'='*60}")
            print(f"[Node {self.node_id}] Starting election for term {current_term}")
            print(f"{'='*60}")

        votes_received = 1
        votes_needed = (len(self.peers) + 1) // 2 + 1

        print(f"[Node {self.node_id}] Voted for self. Votes: 1/{len(self.peers)} (need {votes_needed})")

        for peer_id, peer_addr in self.peers.items():
            if peer_id == self.node_id:
                continue

            try:
                last_log_index = len(self.log)
                last_log_term = self.log[-1].term if self.log else 0

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
                        if self.state != NodeState.CANDIDATE or self.current_term != current_term:
                            return
                        if response.term > self.current_term:
                            self._step_down(response.term)
                            return
                        if response.vote_granted:
                            votes_received += 1
                            print(f"[Node {self.node_id}] Received vote from Node {peer_id}. Votes: {votes_received}/{len(self.peers)}")
                            if votes_received >= votes_needed:
                                self._become_leader()
                                return
            except Exception as e:
                print(f"[Node {self.node_id}] Failed to get vote from Node {peer_id}: {e}")

    def _become_leader(self):
        """Transition to leader state"""
        with self.lock:
            if self.state != NodeState.CANDIDATE:
                return

            print(f"\n{'='*60}")
            print(f"[Node {self.node_id}] WON ELECTION for term {self.current_term}!")
            print(f"[Node {self.node_id}] State: CANDIDATE -> LEADER")
            print(f"{'='*60}\n")

            self.state = NodeState.LEADER
            self.current_leader = self.node_id

            for peer_id in self.peers:
                if peer_id != self.node_id:
                    self.next_index[peer_id] = len(self.log) + 1
                    self.match_index[peer_id] = 0

            if self.heartbeat_thread is None or not self.heartbeat_thread.is_alive():
                self.heartbeat_thread = threading.Thread(target=self._send_heartbeats, daemon=True)
                self.heartbeat_thread.start()

    def _send_heartbeats(self):
        """Leader sends periodic heartbeats and replicates log"""
        while self.running:
            with self.lock:
                if self.state != NodeState.LEADER:
                    return
                current_term = self.current_term

            for peer_id, peer_addr in self.peers.items():
                if peer_id == self.node_id:
                    continue

                try:
                    with self.lock:
                        if self.state != NodeState.LEADER:
                            return

                        next_idx = self.next_index.get(peer_id, 1)
                        prev_log_index = next_idx - 1
                        prev_log_term = self.log[prev_log_index - 1].term if prev_log_index > 0 else 0

                        entries = []
                        if len(self.log) >= next_idx:
                            entries = self.log[next_idx - 1:]

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

                        if entries:
                            print(f"[Node {self.node_id}] Q4 STEP 3: Sending {len(entries)} log entries to Node {peer_id}")

                        msg_type = "AppendEntries (heartbeat)" if not entries else f"AppendEntries ({len(entries)} entries)"
                        print(f"[Node {self.node_id}] sends RPC {msg_type} to Node {peer_id}")

                        response = stub.AppendEntries(request, timeout=0.5)

                        with self.lock:
                            if self.state != NodeState.LEADER:
                                return
                            if response.term > self.current_term:
                                self._step_down(response.term)
                                return
                            if response.success:
                                if entries:
                                    self.match_index[peer_id] = prev_log_index + len(entries)
                                    self.next_index[peer_id] = self.match_index[peer_id] + 1
                                    print(f"[Node {self.node_id}] Q4: Received ACK from Node {peer_id}")
                            else:
                                self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)

                except Exception as e:
                    pass  # Silently ignore network failures

            self._update_commit_index()
            self._apply_committed_entries()
            time.sleep(self.heartbeat_timeout)

    def _update_commit_index(self):
        """Update commit index based on majority replication"""
        with self.lock:
            if self.state != NodeState.LEADER:
                return

            for n in range(len(self.log), self.commit_index, -1):
                if n == 0:
                    break

                replicated_count = 1
                for peer_id in self.peers:
                    if peer_id != self.node_id and self.match_index.get(peer_id, 0) >= n:
                        replicated_count += 1

                majority = (len(self.peers) + 1) // 2 + 1
                if replicated_count >= majority and self.log[n-1].term == self.current_term:
                    if n > self.commit_index:
                        print(f"[Node {self.node_id}] Q4 STEP 6: Received MAJORITY ACKs ({replicated_count}/{len(self.peers)+1} nodes)")
                        print(f"[Node {self.node_id}] Q4 STEP 7: Committing entry {n}")
                        self.commit_index = n
                    break

    def _apply_committed_entries(self):
        """Apply committed log entries to state machine"""
        with self.lock:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log[self.last_applied - 1]

                try:
                    data = json.loads(entry.data)
                    result = self._execute_operation(entry.operation, data)
                    print(f"[Node {self.node_id}] Q4 EXECUTE: Applied entry {self.last_applied}: {entry.operation}")
                except Exception as e:
                    print(f"[Node {self.node_id}] Error applying entry {self.last_applied}: {e}")

    def _execute_operation(self, operation: str, data: dict) -> any:
        """
        Execute operation on voting state machine
        THIS IS WHERE THEIR VOTING LOGIC IS INTEGRATED
        """
        if operation == "CREATE_POLL":
            poll_uuid = data.get("uuid")
            self.state_machine["polls"][poll_uuid] = {
                "poll_questions": data.get("poll_questions"),
                "options": data.get("options"),
                "status": "open",
                "create_at_time": data.get("create_at_time"),
                "votes": {opt: 0 for opt in data.get("options", [])}
            }
            return poll_uuid

        elif operation == "CAST_VOTE":
            poll_uuid = data.get("uuid")
            user_id = data.get("userID")
            option = data.get("select_options")

            # Check duplicate vote
            vote_key = (user_id, poll_uuid)
            if vote_key in self.state_machine["user_votes"]:
                return "duplicate_vote"

            # Record vote
            if poll_uuid in self.state_machine["polls"]:
                if option in self.state_machine["polls"][poll_uuid]["votes"]:
                    self.state_machine["polls"][poll_uuid]["votes"][option] += 1
                    self.state_machine["user_votes"][vote_key] = option
                    return "Vote Successfully!"
            return "Poll Not Found"

        elif operation == "CLOSE_POLL":
            poll_uuid = data.get("uuid")
            if poll_uuid in self.state_machine["polls"]:
                self.state_machine["polls"][poll_uuid]["status"] = "close"
                return "closed"
            return "not_found"

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
                print(f"[Node {self.node_id}] State: {old_state.value} -> FOLLOWER (term {new_term})")

    # ==================== RAFT RPC HANDLERS ====================

    def RequestVote(self, request, context):
        """Handle RequestVote RPC (Q3)"""
        print(f"[Node {self.node_id}] runs RPC RequestVote called by Node {request.candidate_id}")

        with self.lock:
            if request.term > self.current_term:
                self._step_down(request.term)

            vote_granted = False

            if request.term >= self.current_term:
                if self.voted_for is None or self.voted_for == request.candidate_id:
                    our_last_log_index = len(self.log)
                    our_last_log_term = self.log[-1].term if self.log else 0

                    log_ok = (request.last_log_term > our_last_log_term or
                             (request.last_log_term == our_last_log_term and
                              request.last_log_index >= our_last_log_index))

                    if log_ok:
                        vote_granted = True
                        self.voted_for = request.candidate_id
                        self.last_heartbeat = time.time()
                        print(f"[Node {self.node_id}] Granted vote to Node {request.candidate_id}")

            return raft_pb2.RequestVoteResponse(
                term=self.current_term,
                vote_granted=vote_granted
            )

    def AppendEntries(self, request, context):
        """Handle AppendEntries RPC (Q3, Q4)"""
        msg_type = "heartbeat" if not request.entries else f"{len(request.entries)} entries"
        print(f"[Node {self.node_id}] runs RPC AppendEntries ({msg_type}) called by Node {request.leader_id}")

        with self.lock:
            if request.term > self.current_term:
                self._step_down(request.term)

            success = False

            if request.term >= self.current_term:
                self.last_heartbeat = time.time()
                self.current_leader = request.leader_id

                if self.state != NodeState.FOLLOWER:
                    self._step_down(request.term)

                if request.prev_log_index == 0 or \
                   (request.prev_log_index <= len(self.log) and
                    (request.prev_log_index == 0 or self.log[request.prev_log_index - 1].term == request.prev_log_term)):

                    success = True

                    if request.entries:
                        self.log = self.log[:request.prev_log_index]
                        self.log.extend(request.entries)
                        print(f"[Node {self.node_id}] Q4 STEP 4: Follower copied {len(request.entries)} entries to log")

                    if request.leader_commit > self.commit_index:
                        old_commit = self.commit_index
                        self.commit_index = min(request.leader_commit, len(self.log))
                        print(f"[Node {self.node_id}] Q4 STEP 5: Updated commit index c: {old_commit} → {self.commit_index}")
                        self._apply_committed_entries()

            return raft_pb2.AppendEntriesResponse(
                term=self.current_term,
                success=success,
                match_index=len(self.log) if success else 0
            )

    # ==================== THEIR POLLING SERVICE API ====================
    # We keep their gRPC API but use Raft for replication

    def CreatePoll(self, request, context):
        """
        Their CreatePoll RPC - now uses Raft for replication
        Original: microservice_rpc/app/primary_server.py:58
        """
        print(f"[Node {self.node_id}] Received CreatePoll request")

        # Check if leader
        with self.lock:
            if self.state != NodeState.LEADER:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details(f"Not the leader. Try Node {self.current_leader}")
                return polling_pb2.PollResponse()

            # Generate UUID
            poll_uuid = str(uuid_lib.uuid4())
            create_time = time.strftime('%Y-%m-%d %H:%M:%S')

            # Create Raft log entry
            poll_data = {
                "uuid": poll_uuid,
                "poll_questions": request.poll_questions,
                "options": list(request.options),
                "create_at_time": create_time
            }

            new_entry = raft_pb2.LogEntry(
                term=self.current_term,
                index=len(self.log) + 1,
                operation="CREATE_POLL",
                data=json.dumps(poll_data)
            )

            self.log.append(new_entry)
            target_index = len(self.log)

            print(f"[Node {self.node_id}] Q4 STEP 1: Leader received CREATE_POLL request")
            print(f"[Node {self.node_id}] Q4 STEP 2: Appended <CREATE_POLL, t={self.current_term}, k+1={target_index}> to log")

        # Wait for commit
        max_wait = 5.0
        start_time = time.time()

        while time.time() - start_time < max_wait:
            with self.lock:
                if self.commit_index >= target_index:
                    poll = self.state_machine["polls"][poll_uuid]
                    return polling_pb2.PollResponse(
                        uuid=poll_uuid,
                        poll_questions=poll["poll_questions"],
                        options=poll["options"],
                        status=poll["status"],
                        create_at_time=poll["create_at_time"]
                    )
            time.sleep(0.1)

        context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
        context.set_details("Timeout waiting for consensus")
        return polling_pb2.PollResponse()

    def ListPolls(self, request, context):
        """
        Their ListPolls RPC - reads from replicated state
        Original: microservice_rpc/app/primary_server.py:73
        """
        with self.lock:
            polls_list = [
                polling_pb2.PollResponse(
                    uuid=uuid,
                    poll_questions=poll["poll_questions"],
                    options=poll["options"],
                    status=poll["status"],
                    create_at_time=poll["create_at_time"]
                )
                for uuid, poll in self.state_machine["polls"].items()
            ]
            return polling_pb2.ListPollsResponse(polls=polls_list)

    def ClosePoll(self, request, context):
        """
        Their ClosePoll RPC - uses Raft for replication
        Original: microservice_rpc/app/primary_server.py:87
        """
        with self.lock:
            if self.state != NodeState.LEADER:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details(f"Not the leader. Try Node {self.current_leader}")
                return polling_pb2.PollResponse()

            if request.uuid not in self.state_machine["polls"]:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Poll not found")
                return polling_pb2.PollResponse()

            close_data = {"uuid": request.uuid}
            new_entry = raft_pb2.LogEntry(
                term=self.current_term,
                index=len(self.log) + 1,
                operation="CLOSE_POLL",
                data=json.dumps(close_data)
            )
            self.log.append(new_entry)
            target_index = len(self.log)

        # Wait for commit
        max_wait = 5.0
        start_time = time.time()

        while time.time() - start_time < max_wait:
            with self.lock:
                if self.commit_index >= target_index:
                    poll = self.state_machine["polls"][request.uuid]
                    return polling_pb2.PollResponse(
                        uuid=request.uuid,
                        poll_questions=poll["poll_questions"],
                        options=poll["options"],
                        status=poll["status"],
                        create_at_time=poll["create_at_time"]
                    )
            time.sleep(0.1)

        context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
        return polling_pb2.PollResponse()

    def CastVote(self, request, context):
        """
        Their CastVote RPC - uses Raft for replication
        Original: microservice_rpc/app/primary_server.py:109
        """
        # Read-only checks can be done without Raft
        with self.lock:
            if self.state != NodeState.LEADER:
                return polling_pb2.VoteResponse(status=f"Not leader. Try Node {self.current_leader}")

            if request.uuid not in self.state_machine["polls"]:
                return polling_pb2.VoteResponse(status="Poll Not Found")

            poll = self.state_machine["polls"][request.uuid]
            if poll["status"] != "open":
                return polling_pb2.VoteResponse(status="Poll Closed")

            if request.select_options not in poll["options"]:
                return polling_pb2.VoteResponse(status="Invalid Option")

            # Check duplicate (before adding to log)
            vote_key = (request.userID, request.uuid)
            if vote_key in self.state_machine["user_votes"]:
                return polling_pb2.VoteResponse(status="duplicate_vote")

            # Create Raft log entry
            vote_data = {
                "uuid": request.uuid,
                "userID": request.userID,
                "select_options": request.select_options
            }

            new_entry = raft_pb2.LogEntry(
                term=self.current_term,
                index=len(self.log) + 1,
                operation="CAST_VOTE",
                data=json.dumps(vote_data)
            )
            self.log.append(new_entry)
            target_index = len(self.log)

        # Wait for commit
        max_wait = 5.0
        start_time = time.time()

        while time.time() - start_time < max_wait:
            with self.lock:
                if self.commit_index >= target_index:
                    return polling_pb2.VoteResponse(status="Vote Successfully!")
            time.sleep(0.1)

        return polling_pb2.VoteResponse(status="Timeout")

    def GetPollResults(self, request, context):
        """
        Their GetPollResults RPC - reads from replicated state
        Original: microservice_rpc/app/primary_server.py:140
        """
        with self.lock:
            if request.uuid not in self.state_machine["polls"]:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Poll not found")
                return polling_pb2.PollResultResponse()

            poll = self.state_machine["polls"][request.uuid]
            return polling_pb2.PollResultResponse(
                uuid=request.uuid,
                poll_questions=poll["poll_questions"],
                results=poll["votes"]
            )


def serve(node_id: int, port: int, peers: Dict[int, str]):
    """Start the integrated Raft-Polling node"""
    node = RaftPollingNode(node_id, peers)
    node.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Register Raft services
    raft_pb2_grpc.add_RaftNodeServicer_to_server(node, server)

    # Register Polling services (their API)
    polling_pb2_grpc.add_PollServiceServicer_to_server(node, server)
    polling_pb2_grpc.add_VoteServiceServicer_to_server(node, server)
    polling_pb2_grpc.add_ResultServiceServicer_to_server(node, server)

    server.add_insecure_port(f'[::]:{port}')
    server.start()

    print(f"\n[Node {node_id}] Raft-Polling Server started on port {port}")
    print(f"[Node {node_id}] Provides:")
    print(f"[Node {node_id}]   - Raft RPCs (RequestVote, AppendEntries)")
    print(f"[Node {node_id}]   - Polling RPCs (CreatePoll, CastVote, GetPollResults, etc.)")
    print(f"[Node {node_id}] Waiting for election...\n")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        node.running = False
        server.stop(0)
        print(f"\n[Node {node_id}] Shutting down...")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python raft_polling_node.py <node_id>")
        sys.exit(1)

    node_id = int(sys.argv[1])

    # Define 5-node Raft cluster
    peers = {
        1: "node1:50051",
        2: "node2:50052",
        3: "node3:50053",
        4: "node4:50054",
        5: "node5:50055"
    }

    port = 50050 + node_id

    serve(node_id, port, peers)
