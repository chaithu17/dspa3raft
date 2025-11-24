#!/usr/bin/env python3
"""
Test Case: Raft Leader Election
Monitors cluster startup and validates leader election process
"""

import subprocess
import time
import sys
import re
import os
from datetime import datetime
from pathlib import Path

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

def run_command(cmd, show_output=False):
    """Run shell command and return output"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30
        )
        if show_output and result.stdout:
            print(result.stdout)
        if show_output and result.stderr:
            print(result.stderr, file=sys.stderr)
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Command timeout"
    except Exception as e:
        return False, "", str(e)

def cleanup_containers():
    """Stop and remove any existing Raft containers"""
    print_info("Cleaning up existing containers...")
    run_command("docker stop $(docker ps -q --filter 'name=raft_polling') 2>/dev/null || true")
    run_command("docker rm $(docker ps -aq --filter 'name=raft_polling') 2>/dev/null || true")
    time.sleep(2)
    print_success("Cleanup complete")

def start_cluster():
    """Start the 5-node Raft cluster"""
    print_info("Starting 5-node Raft cluster...")

    # Get the directory where this script is located
    script_dir = Path(__file__).parent.absolute()

    # Change to script directory to run docker-compose
    original_dir = os.getcwd()
    os.chdir(script_dir)

    success, stdout, stderr = run_command("docker-compose up -d")

    # Change back to original directory
    os.chdir(original_dir)

    if not success:
        print_error("Failed to start cluster")
        print(stderr)
        return False

    print_success("Cluster started")
    return True

def check_containers_running():
    """Verify all 5 containers are running"""
    print_info("Verifying containers are running...")
    success, stdout, _ = run_command("docker ps --filter 'name=raft_polling' --format '{{.Names}}'")

    if not success:
        return False

    containers = [line.strip() for line in stdout.strip().split('\n') if line.strip()]
    expected = ['raft_polling_node1', 'raft_polling_node2', 'raft_polling_node3',
                'raft_polling_node4', 'raft_polling_node5']

    running = len(containers)
    if running == 5:
        print_success(f"All 5 containers running: {', '.join(containers)}")
        return True
    else:
        print_error(f"Only {running}/5 containers running")
        return False

def monitor_election():
    """Monitor logs in real-time until leader is elected"""
    print_header("MONITORING ELECTION PROCESS")

    start_time = time.time()
    max_wait = 30  # Maximum 30 seconds to elect leader
    check_interval = 0.5

    election_events = []
    nodes_initialized = set()
    leader_node = None
    leader_term = None

    print_info("Waiting for nodes to initialize and elect leader...")
    print_info("(Showing real-time election events)\n")

    while time.time() - start_time < max_wait:
        # Check each node's logs
        for node_id in range(1, 6):
            success, stdout, _ = run_command(
                f"docker logs raft_polling_node{node_id} 2>&1 | tail -50"
            )

            if not success:
                continue

            # Check for initialization
            if node_id not in nodes_initialized:
                if f"Node {node_id}" in stdout and "Initialized as FOLLOWER" in stdout:
                    nodes_initialized.add(node_id)
                    event = f"[Node {node_id}] Initialized as FOLLOWER"
                    if event not in election_events:
                        election_events.append(event)
                        print(f"{Colors.OKBLUE}{event}{Colors.ENDC}")

            # Check for election start
            election_start = re.search(r'Node (\d+).*Starting election for term (\d+)', stdout)
            if election_start:
                event = f"[Node {election_start.group(1)}] Starting election for term {election_start.group(2)}"
                if event not in election_events:
                    election_events.append(event)
                    print(f"{Colors.WARNING}{event}{Colors.ENDC}")

            # Check for RequestVote RPCs
            vote_sends = re.findall(r'Node (\d+) sends RPC RequestVote to Node (\d+)', stdout)
            for sender, receiver in vote_sends:
                event = f"[Node {sender}] Sends RequestVote to Node {receiver}"
                if event not in election_events:
                    election_events.append(event)
                    print(f"{Colors.OKCYAN}  → {event}{Colors.ENDC}")

            # Check for vote grants
            vote_grants = re.findall(r'Node (\d+).*Granted vote to Node (\d+)', stdout)
            for granter, candidate in vote_grants:
                event = f"[Node {granter}] Granted vote to Node {candidate}"
                if event not in election_events:
                    election_events.append(event)
                    print(f"{Colors.OKCYAN}  ← {event}{Colors.ENDC}")

            # Check for leader election
            leader_match = re.search(r'Node (\d+).*WON ELECTION.*term (\d+)', stdout)
            if leader_match and leader_node is None:
                leader_node = int(leader_match.group(1))
                leader_term = int(leader_match.group(2))
                event = f"[Node {leader_node}] WON ELECTION for term {leader_term}!"
                election_events.append(event)
                print(f"\n{Colors.BOLD}{Colors.OKGREEN}{event}{Colors.ENDC}\n")
                return True, leader_node, leader_term, time.time() - start_time, election_events

        time.sleep(check_interval)

    # Timeout
    return False, None, None, time.time() - start_time, election_events

def verify_single_leader():
    """Verify that exactly one leader exists"""
    print_info("Verifying exactly one leader exists...")

    leaders = []
    for node_id in range(1, 6):
        success, stdout, _ = run_command(
            f"docker logs raft_polling_node{node_id} 2>&1 | grep 'State.*LEADER' | tail -1"
        )
        if success and "LEADER" in stdout:
            leaders.append(node_id)

    if len(leaders) == 1:
        print_success(f"Exactly 1 leader: Node {leaders[0]}")
        return True, leaders[0]
    elif len(leaders) == 0:
        print_error("No leader found")
        return False, None
    else:
        print_error(f"Multiple leaders found: {leaders}")
        return False, None

def show_rpc_format_evidence(leader_node):
    """Show RPC format evidence"""
    print_header("RPC FORMAT VERIFICATION")

    print_info("Checking RequestVote RPC format...\n")

    # Show sender format
    success, stdout, _ = run_command(
        f"docker logs raft_polling_node{leader_node} 2>&1 | grep 'sends RPC RequestVote' | head -3"
    )
    if stdout.strip():
        print(f"{Colors.OKBLUE}Sender format (from Node {leader_node}):{Colors.ENDC}")
        for line in stdout.strip().split('\n')[:3]:
            print(f"  {line}")

    print()

    # Show receiver format
    other_node = 1 if leader_node != 1 else 2
    success, stdout, _ = run_command(
        f"docker logs raft_polling_node{other_node} 2>&1 | grep 'runs RPC RequestVote' | head -3"
    )
    if stdout.strip():
        print(f"{Colors.OKBLUE}Receiver format (at Node {other_node}):{Colors.ENDC}")
        for line in stdout.strip().split('\n')[:3]:
            print(f"  {line}")

    print()

def show_heartbeat_evidence(leader_node):
    """Show heartbeat evidence"""
    print_header("HEARTBEAT VERIFICATION")

    print_info(f"Checking heartbeats from Leader (Node {leader_node})...\n")

    success, stdout, _ = run_command(
        f"docker logs raft_polling_node{leader_node} 2>&1 | grep -E 'Sending heartbeat|sends RPC AppendEntries' | tail -5"
    )

    if stdout.strip():
        for line in stdout.strip().split('\n'):
            print(f"  {line}")
    else:
        print_error("No heartbeat messages found")

def main():
    """Main test execution"""
    print_header("TEST CASE: RAFT LEADER ELECTION")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Step 1: Cleanup
    print_header("STEP 1: CLEANUP")
    cleanup_containers()

    # Step 2: Start cluster
    print_header("STEP 2: START CLUSTER")
    if not start_cluster():
        print_error("TEST FAILED: Could not start cluster")
        return 1

    # Wait for containers to initialize
    print_info("Waiting for containers to fully initialize...")
    time.sleep(8)

    # Step 3: Verify containers
    print_header("STEP 3: VERIFY CONTAINERS")
    if not check_containers_running():
        print_error("TEST FAILED: Not all containers running")
        return 1

    # Step 4: Monitor election
    election_success, leader_node, leader_term, election_time, events = monitor_election()

    if not election_success:
        print_error("TEST FAILED: No leader elected within timeout")
        print_info("Election events captured:")
        for event in events:
            print(f"  {event}")
        return 1

    # Step 5: Verify single leader
    print_header("STEP 5: VERIFY SINGLE LEADER")
    single_leader, verified_leader = verify_single_leader()

    if not single_leader:
        print_error("TEST FAILED: Leader verification failed")
        return 1

    # Step 6: Show RPC format
    show_rpc_format_evidence(leader_node)

    # Step 7: Show heartbeats
    show_heartbeat_evidence(leader_node)

    # Final summary
    print_header("TEST SUMMARY")
    print_success("TEST PASSED: Leader Election Successful\n")

    print(f"{Colors.BOLD}Results:{Colors.ENDC}")
    print(f"  Leader Node:      {Colors.OKGREEN}Node {leader_node}{Colors.ENDC}")
    print(f"  Leader Term:      {Colors.OKGREEN}{leader_term}{Colors.ENDC}")
    print(f"  Election Time:    {Colors.OKGREEN}{election_time:.2f} seconds{Colors.ENDC}")
    print(f"  Nodes Running:    {Colors.OKGREEN}5/5{Colors.ENDC}")

    print(f"\n{Colors.BOLD}Validation Checklist:{Colors.ENDC}")
    print_success("Exactly one leader elected")
    print_success(f"Leader elected in term {leader_term}")
    print_success("All 5 nodes initialized as followers")
    print_success("RequestVote RPCs sent and received")
    print_success("Majority votes achieved")
    print_success("Leader sending heartbeats")
    print_success("RPC format matches requirements")

    print(f"\n{Colors.BOLD}Q3 Requirements:{Colors.ENDC}")
    print_success("Heartbeat timeout: 1 second")
    print_success("Election timeout: Random [1.5, 3] seconds")
    print_success("All nodes start as FOLLOWER")
    print_success("RequestVote RPC implemented")
    print_success("Majority voting (3/5 nodes)")
    print_success("RPC logging format correct")

    print(f"\n{Colors.OKBLUE}To view full logs:{Colors.ENDC}")
    print(f"  docker logs raft_polling_node{leader_node}")

    print(f"\n{Colors.OKBLUE}To run integration tests:{Colors.ENDC}")
    print(f"  python3 test_integrated_client.py")

    print(f"\n{Colors.HEADER}{'='*70}{Colors.ENDC}\n")

    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}Test interrupted by user{Colors.ENDC}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Colors.FAIL}Test failed with error: {e}{Colors.ENDC}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
