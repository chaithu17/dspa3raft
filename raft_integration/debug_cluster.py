#!/usr/bin/env python3
"""
Debug script to check container logs and status
"""

import subprocess
import time

def run_command(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.returncode == 0, result.stdout, result.stderr

print("=" * 70)
print("DEBUGGING RAFT CLUSTER")
print("=" * 70)
print()

# Check container status
print("1. Container Status:")
print("-" * 70)
success, stdout, _ = run_command("docker ps --filter 'name=raft_polling' --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'")
print(stdout)

# Check if containers are healthy
print("\n2. Checking for errors in containers:")
print("-" * 70)
for node_id in range(1, 6):
    print(f"\n--- Node {node_id} ---")
    success, stdout, stderr = run_command(f"docker logs raft_polling_node{node_id} 2>&1 | tail -20")
    if stdout.strip():
        print(stdout)
    else:
        print(f"No logs found for Node {node_id}")

    if stderr.strip():
        print(f"STDERR: {stderr}")

print("\n" + "=" * 70)
print("CHECKING FOR INITIALIZATION")
print("=" * 70)
for node_id in range(1, 6):
    success, stdout, _ = run_command(f"docker logs raft_polling_node{node_id} 2>&1 | grep -i 'initialized\\|error\\|exception' | head -5")
    if stdout.strip():
        print(f"\nNode {node_id}:")
        print(stdout)

print("\n" + "=" * 70)
print("DOCKER COMPOSE STATUS")
print("=" * 70)
success, stdout, _ = run_command("docker-compose ps")
print(stdout)

print("\n" + "=" * 70)
print("SUGGESTION")
print("=" * 70)
print("If you see errors above, try:")
print("1. docker-compose down")
print("2. docker-compose up -d")
print("3. docker logs raft_polling_node1 -f  (to follow logs)")
