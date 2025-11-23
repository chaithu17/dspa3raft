#!/usr/bin/env python3
"""Add just the forwarding helper and update three methods"""

import re

with open('raft_polling_node.py', 'r') as f:
    content = f.read()

# 1. Add helper method after _step_down
helper_method = '''
    def _forward_to_leader(self, service_stub_class, request):
        """Forward request to leader (Q4 requirement)"""
        with self.lock:
            leader_id = self.current_leader
            if leader_id is None or leader_id not in self.peers:
                return None
            leader_addr = self.peers[leader_id]
            print(f"[Node {self.node_id}] Forwarding to leader Node {leader_id}")
        
        try:
            with grpc.insecure_channel(leader_addr) as channel:
                stub = service_stub_class(channel)
                method_name = type(request).__name__.replace('Request', '')
                return getattr(stub, method_name)(request, timeout=5.0)
        except Exception as e:
            print(f"[Node {self.node_id}] Forward failed: {e}")
            return None
'''

# Insert helper after _step_down method
if '_forward_to_leader' not in content:
    content = re.sub(
        r'(def _step_down\(self.*?\n(?:.*?\n)*?            if old_state.*?\n\n)',
        r'\1' + helper_method + '\n',
        content,
        count=1
    )

# 2. Fix CreatePoll - simple version
content = re.sub(
    r'(def CreatePoll\(self, request, context\):.*?if self\.state != NodeState\.LEADER:)\s*context\.set_code.*?return polling_pb2\.PollResponse\(\)',
    r'''\1
                # Forward to leader
                resp = self._forward_to_leader(polling_pb2_grpc.PollServiceStub, request)
                if resp: return resp
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("No leader available")
                return polling_pb2.PollResponse()''',
    content,
    flags=re.DOTALL
)

# 3. Fix CastVote
content = re.sub(
    r'(if self\.state != NodeState\.LEADER:)\s*return polling_pb2\.VoteResponse\(status=f"Not leader.*?\"\)',
    r'''\1
                # Forward to leader
                resp = self._forward_to_leader(polling_pb2_grpc.VoteServiceStub, request)
                if resp: return resp
                return polling_pb2.VoteResponse(status="No leader")''',
    content
)

# 4. Fix ClosePoll
content = re.sub(
    r'(def ClosePoll.*?if self\.state != NodeState\.LEADER:)\s*context\.set_code.*?return polling_pb2\.PollResponse\(\)',
    r'''\1
                # Forward to leader
                resp = self._forward_to_leader(polling_pb2_grpc.PollServiceStub, request)
                if resp: return resp
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("No leader available")
                return polling_pb2.PollResponse()''',
    content,
    flags=re.DOTALL
)

with open('raft_polling_node.py', 'w') as f:
    f.write(content)

print("✅ Done!")
