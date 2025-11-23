#!/usr/bin/env python3
"""
Enhanced logging additions for Q4 demonstration
Add these to your existing raft_node.py
"""

# ============================================================================
# Add to ClientRequest method (around line 460)
# ============================================================================

def ClientRequest(self, request, context):
    """Handle client requests (Q4) - ENHANCED LOGGING"""
    print(f"[Node {self.node_id}] runs RPC ClientRequest called by client")

    with self.lock:
        # Q4 STEP: Check if leader, redirect if not
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

        # Q4 STEP 2: Append <o, t, k+1> to log
        new_entry = raft_pb2.LogEntry(
            term=self.current_term,        # t = current term
            index=len(self.log) + 1,       # k+1 = next index
            operation=request.operation,    # o = operation
            data=request.data
        )

        self.log.append(new_entry)
        target_index = len(self.log)

        print(f"[Node {self.node_id}] Q4 STEP 2: Appended <{request.operation}, t={self.current_term}, k+1={target_index}> to log")
        print(f"[Node {self.node_id}]           Log size: {len(self.log)}, Commit index c={self.commit_index}")
        print(f"[Node {self.node_id}]           Entry details: index={new_entry.index}, term={new_entry.term}, op={new_entry.operation}")

    # Q4 STEP 3 happens in _send_heartbeats (next heartbeat will send this)
    print(f"[Node {self.node_id}] Q4 STEP 3: Will send log to all followers on next heartbeat...")

    # Wait for replication and commitment
    max_wait = 5.0
    start_time = time.time()

    while time.time() - start_time < max_wait:
        with self.lock:
            if self.commit_index >= target_index:
                print(f"[Node {self.node_id}] Q4 FINAL: Operation committed at index {target_index}")
                print(f"[Node {self.node_id}]          Commit index c incremented: {self.commit_index-1} → {self.commit_index}")
                result_data = json.dumps({"status": "committed", "index": target_index})
                return raft_pb2.ClientRequestResponse(
                    success=True,
                    message=f"Operation committed at index {target_index}",
                    result=result_data,
                    leader_id=self.node_id
                )
        time.sleep(0.1)

    return raft_pb2.ClientRequestResponse(
        success=False,
        message="Timeout waiting for commit",
        result="",
        leader_id=self.node_id
    )


# ============================================================================
# Add to _send_heartbeats method (around line 240)
# ============================================================================

# In the AppendEntries sending section, add:

                    # Q4 LOGGING: Show what we're sending
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

                        if response.term > self.current_term:
                            self._step_down(response.term)
                            return

                        # Q4 STEP: Track ACKs
                        if response.success:
                            if entries:
                                self.match_index[peer_id] = prev_log_index + len(entries)
                                self.next_index[peer_id] = self.match_index[peer_id] + 1
                                print(f"[Node {self.node_id}] Q4: Received ACK from Node {peer_id} (replicated up to {self.match_index[peer_id]})")


# ============================================================================
# Add to AppendEntries method (around line 437)
# ============================================================================

                # Q4 STEP: Follower copies log
                if request.entries:
                    self.log = self.log[:request.prev_log_index]
                    self.log.extend(request.entries)
                    print(f"[Node {self.node_id}] Q4 STEP 4: Follower copied {len(request.entries)} entries to log")
                    print(f"[Node {self.node_id}]            Log size now: {len(self.log)}")
                    for entry in request.entries:
                        print(f"[Node {self.node_id}]            - Entry {entry.index}: {entry.operation} (term {entry.term})")

                # Q4 STEP: Update commit index and execute
                if request.leader_commit > self.commit_index:
                    old_commit = self.commit_index
                    self.commit_index = min(request.leader_commit, len(self.log))
                    print(f"[Node {self.node_id}] Q4 STEP 5: Updated commit index c: {old_commit} → {self.commit_index}")
                    print(f"[Node {self.node_id}]            Will execute operations up to index {self.commit_index}")

                    # Apply committed entries
                    self._apply_committed_entries()


# ============================================================================
# Add to _apply_committed_entries (around line 315)
# ============================================================================

def _apply_committed_entries(self):
    """Apply committed log entries to state machine - ENHANCED LOGGING"""
    with self.lock:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]

            try:
                data = json.loads(entry.data)
                result = self._execute_operation(entry.operation, data)
                print(f"[Node {self.node_id}] Q4 EXECUTE: Applied entry {self.last_applied}: {entry.operation}")
                print(f"[Node {self.node_id}]             Data: {entry.data[:80]}")
            except Exception as e:
                print(f"[Node {self.node_id}] Error applying entry {self.last_applied}: {e}")


# ============================================================================
# Add to _update_commit_index (around line 300)
# ============================================================================

def _update_commit_index(self):
    """Update commit index based on majority replication (Q4) - ENHANCED LOGGING"""
    with self.lock:
        if self.state != NodeState.LEADER:
            return

        for n in range(len(self.log), self.commit_index, -1):
            if n == 0:
                break

            # Q4: Count replicas
            replicated_count = 1  # Leader has it
            for peer_id in self.peers:
                if peer_id != self.node_id and self.match_index.get(peer_id, 0) >= n:
                    replicated_count += 1

            majority = (len(self.peers) + 1) // 2 + 1

            if replicated_count >= majority and self.log[n-1].term == self.current_term:
                if n > self.commit_index:
                    print(f"[Node {self.node_id}] Q4 STEP 6: Received MAJORITY ACKs ({replicated_count}/{len(self.peers)+1} nodes)")
                    print(f"[Node {self.node_id}] Q4 STEP 7: Committing entry {n} (was pending, now committed)")
                    print(f"[Node {self.node_id}]            Incrementing c: {self.commit_index} → {n}")
                    self.commit_index = n

                    # Execute pending operations
                    self._apply_committed_entries()
                break
