#!/usr/bin/env python3
"""
Extract COMMIT portions from trace files.

This script processes all trace files in the traces/ directory and extracts
the COMMIT portion of each transaction trace. The extracted commits are saved
to the extracted_commits/ directory with the format extracted_commit_<number>.txt.

Each extracted file includes:
1. The first line of the original trace (for transaction duration info)
2. The COMMIT portion starting from "portal resolved to: ‹COMMIT TRANSACTION›"

Options:
- --min-duration: Minimum COMMIT duration in milliseconds (default: 50ms)
  Only commits that ran for at least this duration will be extracted.
"""

import os
import re
import glob
import argparse
from pathlib import Path


def extract_commit_from_trace(trace_file_path):
    """
    Extract the COMMIT portion from a trace file.
    
    Args:
        trace_file_path (str): Path to the trace file
        
    Returns:
        tuple: (first_line, commit_section, commit_duration) or (None, None, None) if no commit found
    """
    try:
        with open(trace_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        if not lines:
            return None, None, None
        
        # Get the first line (transaction duration info)
        first_line = lines[0].strip()
        
        # Find the start of the COMMIT section
        commit_start_idx = None
        for i, line in enumerate(lines):
            if "portal resolved to: ‹COMMIT TRANSACTION›" in line:
                commit_start_idx = i
                break
        
        if commit_start_idx is None:
            return None, None, None
        
        # Extract from the commit start to the end
        commit_section = lines[commit_start_idx:]
        
        # Calculate COMMIT duration
        commit_duration = calculate_commit_duration(commit_section)
        
        return first_line, commit_section, commit_duration
        
    except Exception as e:
        print(f"Error processing {trace_file_path}: {e}")
        return None, None, None


def calculate_commit_duration(commit_section):
    """
    Calculate the duration of the COMMIT operation.
    
    Args:
        commit_section (list): List of lines containing the COMMIT section
        
    Returns:
        float: Duration in milliseconds, or None if calculation fails
    """
    try:
        if not commit_section:
            return None
        
        # Find the start time (first line with timestamp)
        start_time = None
        for line in commit_section:
            # Look for timestamp pattern like "985.822ms"
            match = re.search(r'^\s*(\d+\.\d+)ms', line)
            if match:
                start_time = float(match.group(1))
                break
        
        if start_time is None:
            return None
        
        # Find the end time (last line with timestamp)
        end_time = None
        for line in reversed(commit_section):
            # Look for timestamp pattern like "999.640ms"
            match = re.search(r'^\s*(\d+\.\d+)ms', line)
            if match:
                end_time = float(match.group(1))
                break
        
        if end_time is None:
            return None
        
        # Calculate duration
        duration = end_time - start_time
        return duration
        
    except Exception as e:
        print(f"Error calculating commit duration: {e}")
        return None


IGNORED_LOG_PATTERNS = [
    re.compile(r'making txn commit explicit'),
    re.compile(r'looking up descriptors for ids'),
    re.compile(r'event:kv/kvclient/kvcoord/transport\.go:207.*‹received batch response›'),
    # Add more patterns here as needed
]


def analyze_commit_timing(commit_section):
    """
    Analyze the timing patterns in the COMMIT section to identify the longest step, ignoring certain log lines.
    
    Args:
        commit_section (list): List of lines containing the COMMIT section
        
    Returns:
        dict: Analysis results with timing information and category
    """
    try:
        if not commit_section:
            return None
        
        # Parse all timing information from the commit section
        timing_events = []
        
        for line in commit_section:
            # Look for timestamp pattern like "267.242ms    214.906ms"
            match = re.search(r'^\s*(\d+\.\d+)ms\s+(\d+\.\d+)ms', line)
            if match:
                total_time = float(match.group(1))
                step_time = float(match.group(2))
                # Check if this line should be ignored
                ignore = any(p.search(line) for p in IGNORED_LOG_PATTERNS)
                timing_events.append({
                    'total_time': total_time,
                    'step_time': step_time,
                    'line': line.strip(),
                    'ignore': ignore
                })
        
        if not timing_events:
            return None
        
        # Find the step with the longest duration, ignoring excluded lines
        filtered_events = [e for e in timing_events if not e['ignore']]
        if filtered_events:
            longest_step = max(filtered_events, key=lambda x: x['step_time'])
        else:
            # If all are ignored, fallback to the original list
            longest_step = max(timing_events, key=lambda x: x['step_time'])
        
        # Check if this is a QueryIntent step
        is_query_intent = False
        if 'received pre-commit QueryIntent batch response' in longest_step['line']:
            is_query_intent = True
        
        return {
            'longest_step_time': longest_step['step_time'],
            'longest_step_line': longest_step['line'],
            'is_query_intent': is_query_intent,
            'all_timing_events': timing_events
        }
        
    except Exception as e:
        print(f"Error analyzing commit timing: {e}")
        return None


def analyze_network_timing(commit_section, network_threshold, abnormal_threshold=5.0):
    """
    Analyze network timing patterns in the COMMIT section.
    
    Args:
        commit_section (list): List of lines containing the COMMIT section
        network_threshold (float): Threshold in milliseconds for network operations
        abnormal_threshold (float): Threshold in milliseconds for abnormal network patterns
        
    Returns:
        dict: Network analysis results with timing information and node info
    """
    try:
        if not commit_section:
            return None
        
        client_server_pairs = []
        server_client_pairs = []
        
        # Find all client->server pairs
        for i, line in enumerate(commit_section):
            # Look for client operation
            client_match = re.search(r'(\d+\.\d+)ms\s+(\d+\.\d+)ms\s+=== operation:/cockroach\.roachpb\.Internal/Batch _verbose:‹1› node:‹(\d+)›.*span\.kind:‹client›', line)
            if client_match:
                client_timestamp = float(client_match.group(1))
                client_duration = float(client_match.group(2))
                client_node = int(client_match.group(3))
                
                # Look for corresponding server operation
                for j in range(i + 1, len(commit_section)):
                    server_line = commit_section[j]
                    server_match = re.search(r'(\d+\.\d+)ms\s+(\d+\.\d+)ms\s+=== operation:/cockroach\.roachpb\.Internal/Batch _verbose:‹1› node:‹(\d+)› span\.kind:‹server›', server_line)
                    if server_match:
                        server_timestamp = float(server_match.group(1))
                        server_duration = float(server_match.group(2))
                        server_node = int(server_match.group(3))
                        
                        # Calculate client->server latency (server timestamp - client timestamp)
                        client_server_latency = server_timestamp - client_timestamp
                        
                        client_server_pairs.append({
                            'client_timestamp': client_timestamp,
                            'server_timestamp': server_timestamp,
                            'client_node': client_node,
                            'server_node': server_node,
                            'latency': client_server_latency,
                            'client_line': line.strip(),
                            'server_line': server_line.strip()
                        })
                        break
        
        # Find all server->client pairs
        for i, line in enumerate(commit_section):
            # Look for "node sending response"
            server_response_match = re.search(r'(\d+\.\d+)ms\s+(\d+\.\d+)ms\s+event:server/node\.go:1472 \[n(\d+)\] node sending response', line)
            if server_response_match:
                server_response_timestamp = float(server_response_match.group(1))
                server_response_duration = float(server_response_match.group(2))
                server_node = int(server_response_match.group(3))
                
                # Look for corresponding "received batch response"
                for j in range(i + 1, len(commit_section)):
                    client_line = commit_section[j]
                    client_received_match = re.search(r'(\d+\.\d+)ms\s+(\d+\.\d+)ms\s+event:kv/kvclient/kvcoord/transport\.go:207 \[n(\d+),client=.*?\] ‹received batch response›', client_line)
                    if client_received_match:
                        client_received_timestamp = float(client_received_match.group(1))
                        client_received_duration = float(client_received_match.group(2))
                        client_node = int(client_received_match.group(3))
                        
                        # Calculate server->client latency (client received - server response)
                        server_client_latency = client_received_timestamp - server_response_timestamp
                        
                        server_client_pairs.append({
                            'server_response_timestamp': server_response_timestamp,
                            'client_received_timestamp': client_received_timestamp,
                            'server_node': server_node,
                            'client_node': client_node,
                            'latency': server_client_latency,
                            'server_line': line.strip(),
                            'client_line': client_line.strip()
                        })
                        break
        
        # Find the longest client->server latency
        longest_client_server = None
        for pair in client_server_pairs:
            if longest_client_server is None or pair['latency'] > longest_client_server['latency']:
                longest_client_server = pair
        
        # Find the longest server->client latency
        longest_server_client = None
        for pair in server_client_pairs:
            if longest_server_client is None or pair['latency'] > longest_server_client['latency']:
                longest_server_client = pair
        
        # Check for abnormal patterns (significant discrepancy between client->server and server->client)
        abnormal_network = None
        if longest_client_server and longest_server_client:
            # Check if they correspond to the same round trip (same client and server nodes)
            if (longest_client_server['client_node'] == longest_server_client['client_node'] and 
                longest_client_server['server_node'] == longest_server_client['server_node']):
                discrepancy = abs(longest_client_server['latency'] - longest_server_client['latency'])
                if discrepancy > abnormal_threshold:
                    abnormal_network = {
                        'client_server_latency': longest_client_server['latency'],
                        'server_client_latency': longest_server_client['latency'],
                        'discrepancy': discrepancy,
                        'server_node': longest_client_server['server_node'],
                        'client_node': longest_client_server['client_node'],
                        'client_server_pair': longest_client_server,
                        'server_client_pair': longest_server_client
                    }
        
        return {
            'longest_client_server': longest_client_server,
            'longest_server_client': longest_server_client,
            'abnormal_network': abnormal_network,
            'client_server_pairs': client_server_pairs,
            'server_client_pairs': server_client_pairs
        }
        
    except Exception as e:
        print(f"Error analyzing network timing: {e}")
        return None


def analyze_raft_timing(commit_section, raft_threshold):
    """
    Analyze Raft timing patterns in the COMMIT section.
    
    Args:
        commit_section (list): List of lines containing the COMMIT section
        raft_threshold (float): Threshold in milliseconds for Raft operations
        
    Returns:
        dict: Raft analysis results with timing information and node info
    """
    try:
        if not commit_section:
            return None
        
        # Look for Raft-related operations
        raft_operations = []
        
        for line in commit_section:
            # Look for "submitting proposal to proposal buffer"
            if 'event:kv/kvserver/replica_raft.go:430' in line and 'submitting proposal to proposal buffer' in line:
                # Extract timestamp and duration
                match = re.search(r'^\s*(\d+\.\d+)ms\s+(\d+\.\d+)ms', line)
                if match:
                    timestamp = float(match.group(1))
                    duration = float(match.group(2))
                    # Extract node number
                    node_match = re.search(r'\[n(\d+)', line)
                    node = node_match.group(1) if node_match else None
                    raft_operations.append({
                        'type': 'submitting_proposal',
                        'timestamp': timestamp,
                        'duration': duration,
                        'node': node,
                        'line': line.strip()
                    })
            
            # Look for "flushing proposal to Raft"
            elif 'event:kv/kvserver/replica_proposal_buf.go:612' in line and 'flushing proposal to Raft' in line:
                # Extract timestamp and duration
                match = re.search(r'^\s*(\d+\.\d+)ms\s+(\d+\.\d+)ms', line)
                if match:
                    timestamp = float(match.group(1))
                    duration = float(match.group(2))
                    # Extract node number
                    node_match = re.search(r'\[n(\d+)', line)
                    node = node_match.group(1) if node_match else None
                    raft_operations.append({
                        'type': 'flushing_proposal',
                        'timestamp': timestamp,
                        'duration': duration,
                        'node': node,
                        'line': line.strip()
                    })
            
            # Look for local proposal operations
            elif '=== operation:local proposal _verbose' in line:
                # Extract timestamp and duration
                match = re.search(r'^\s*(\d+\.\d+)ms\s+(\d+\.\d+)ms', line)
                if match:
                    timestamp = float(match.group(1))
                    duration = float(match.group(2))
                    # Extract node number
                    node_match = re.search(r'node:‹(\d+)›', line)
                    node = node_match.group(1) if node_match else None
                    raft_operations.append({
                        'type': 'local_proposal',
                        'timestamp': timestamp,
                        'duration': duration,
                        'node': node,
                        'line': line.strip()
                    })
            
            # Look for "applying command"
            elif 'event:kv/kvserver/app_batch.go:116' in line and 'applying command' in line:
                # Extract timestamp and duration
                match = re.search(r'^\s*(\d+\.\d+)ms\s+(\d+\.\d+)ms', line)
                if match:
                    timestamp = float(match.group(1))
                    duration = float(match.group(2))
                    # Extract node number
                    node_match = re.search(r'\[n(\d+)', line)
                    node = node_match.group(1) if node_match else None
                    raft_operations.append({
                        'type': 'applying_command',
                        'timestamp': timestamp,
                        'duration': duration,
                        'node': node,
                        'line': line.strip()
                    })
            
            # Look for "LocalResult"
            elif 'event:kv/kvserver/replica_application_state_machine.go:185' in line and 'LocalResult' in line:
                # Extract timestamp and duration
                match = re.search(r'^\s*(\d+\.\d+)ms\s+(\d+\.\d+)ms', line)
                if match:
                    timestamp = float(match.group(1))
                    duration = float(match.group(2))
                    # Extract node number
                    node_match = re.search(r'\[n(\d+)', line)
                    node = node_match.group(1) if node_match else None
                    raft_operations.append({
                        'type': 'local_result',
                        'timestamp': timestamp,
                        'duration': duration,
                        'node': node,
                        'line': line.strip()
                    })
        
        # Group Raft operations by node and calculate cumulative duration
        raft_by_node = {}
        for raft_op in raft_operations:
            node = raft_op['node']
            if node not in raft_by_node:
                raft_by_node[node] = {
                    'node': node,
                    'operations': [],
                    'total_duration': 0.0
                }
            raft_by_node[node]['operations'].append(raft_op)
            raft_by_node[node]['total_duration'] += raft_op['duration']
        
        # Find the node with the highest cumulative Raft duration
        longest_raft = None
        for node_data in raft_by_node.values():
            if node_data['total_duration'] > raft_threshold:
                if longest_raft is None or node_data['total_duration'] > longest_raft['total_duration']:
                    longest_raft = node_data
        
        return {
            'longest_raft': longest_raft,
            'raft_operations': raft_operations,
            'raft_by_node': raft_by_node
        }
        
    except Exception as e:
        print(f"Error analyzing Raft timing: {e}")
        return None


def analyze_store_send_timing(commit_section, store_send_threshold):
    """
    Analyze store_send timing patterns in the COMMIT section.
    
    Args:
        commit_section (list): List of lines containing the COMMIT section
        store_send_threshold (float): Threshold in milliseconds for store_send operations
        
    Returns:
        dict: Store_send analysis results with timing information and node info
    """
    try:
        if not commit_section:
            return None
        
        # Look for store_send operations
        store_send_operations = []
        
        for line in commit_section:
            # Look for store_send operations
            if 'event:kv/kvserver/store_send.go:149' in line and 'executing' in line:
                # Extract timestamp and duration
                match = re.search(r'^\s*(\d+\.\d+)ms\s+(\d+\.\d+)ms', line)
                if match:
                    timestamp = float(match.group(1))
                    duration = float(match.group(2))
                    # Extract node number
                    node_match = re.search(r'\[n(\d+)', line)
                    node = node_match.group(1) if node_match else None
                    store_send_operations.append({
                        'timestamp': timestamp,
                        'duration': duration,
                        'node': node,
                        'line': line.strip()
                    })
        
        # Find the longest store_send operation
        longest_store_send = None
        for store_send_op in store_send_operations:
            if store_send_op['duration'] > store_send_threshold:
                if longest_store_send is None or store_send_op['duration'] > longest_store_send['duration']:
                    longest_store_send = store_send_op
        
        return {
            'longest_store_send': longest_store_send,
            'store_send_operations': store_send_operations
        }
        
    except Exception as e:
        print(f"Error analyzing store_send timing: {e}")
        return None


def analyze_replica_send_timing(commit_section, replica_send_threshold):
    """
    Analyze replica_send timing patterns in the COMMIT section.
    
    Args:
        commit_section (list): List of lines containing the COMMIT section
        replica_send_threshold (float): Threshold in milliseconds for replica_send operations
        
    Returns:
        dict: Replica_send analysis results with timing information and node info
    """
    try:
        if not commit_section:
            return None
        
        # Look for replica_send operations
        replica_send_operations = []
        
        for line in commit_section:
            # Look for replica_send operations
            if 'event:kv/kvserver/replica_send.go:182' in line and 'read-write path' in line:
                # Extract timestamp and duration
                match = re.search(r'^\s*(\d+\.\d+)ms\s+(\d+\.\d+)ms', line)
                if match:
                    timestamp = float(match.group(1))
                    duration = float(match.group(2))
                    # Extract node number
                    node_match = re.search(r'\[n(\d+)', line)
                    node = node_match.group(1) if node_match else None
                    replica_send_operations.append({
                        'timestamp': timestamp,
                        'duration': duration,
                        'node': node,
                        'line': line.strip()
                    })
        
        # Find the longest replica_send operation
        longest_replica_send = None
        for replica_send_op in replica_send_operations:
            if replica_send_op['duration'] > replica_send_threshold:
                if longest_replica_send is None or replica_send_op['duration'] > longest_replica_send['duration']:
                    longest_replica_send = replica_send_op
        
        return {
            'longest_replica_send': longest_replica_send,
            'replica_send_operations': replica_send_operations
        }
        
    except Exception as e:
        print(f"Error analyzing replica_send timing: {e}")
        return None


def main():
    """Main function to process all trace files."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract COMMIT portions from trace files')
    parser.add_argument('--debug-zip', '-d', default="~/Downloads/debug 4", 
                       help='Path to debug zip directory (default: ~/Downloads/debug 4)')
    parser.add_argument('--min-duration', type=float, default=50.0,
                       help='Minimum COMMIT duration in milliseconds (default: 50ms)')
    parser.add_argument('--max-duration', type=float, default=150.0,
                       help='Maximum COMMIT duration in milliseconds (default: 150ms)')
    parser.add_argument('--network-threshold', type=float, default=50.0,
                       help='Network threshold in milliseconds for categorizing network operations (default: 50ms)')
    parser.add_argument('--raft-threshold', type=float, default=40.0,
                       help='Raft threshold in milliseconds for categorizing Raft operations (default: 40ms)')
    parser.add_argument('--store-send-threshold', type=float, default=10.0,
                       help='Store_send threshold in milliseconds for categorizing store_send operations (default: 10ms)')
    parser.add_argument('--replica-send-threshold', type=float, default=10.0,
                       help='Replica_send threshold in milliseconds for categorizing replica_send operations (default: 10ms)')
    args = parser.parse_args()
    
    min_duration = args.min_duration
    max_duration = args.max_duration
    network_threshold = args.network_threshold
    raft_threshold = args.raft_threshold
    store_send_threshold = args.store_send_threshold
    replica_send_threshold = args.replica_send_threshold
    
    # Expand user path if needed
    debug_zip_path = os.path.expanduser(args.debug_zip)
    
    if not os.path.exists(debug_zip_path):
        print(f"Error: Debug zip directory '{debug_zip_path}' does not exist")
        exit(1)
    
    # Extract debug zip name from path
    debug_zip_name = os.path.basename(debug_zip_path)
    
    # Check if the corresponding traces directory exists
    traces_dir = Path("bin") / debug_zip_name / "traces"
    if not traces_dir.exists():
        print(f"Error: Traces directory '{traces_dir}' does not exist")
        print(f"Please run analyze_debug_zip.py first to extract traces from '{debug_zip_path}'")
        exit(1)
    
    # Get all trace files
    trace_files = list(traces_dir.glob("trace_*.txt"))
    trace_files.sort()  # Sort to ensure consistent ordering
    
    if not trace_files:
        print(f"No trace files found in {traces_dir} directory")
        return
    
    # Create output directory in the same directory as traces
    output_dir = traces_dir.parent / "extracted_commits"
    output_dir.mkdir(exist_ok=True)
    
    print(f"Found {len(trace_files)} trace files to process...")
    print(f"COMMIT duration filter: {min_duration}ms - {max_duration}ms")
    
    processed_count = 0
    skipped_count = 0
    filtered_count = 0
    
    # Collect all valid commits with their durations for sorting
    valid_commits = []
    
    for trace_file in trace_files:
        # Extract trace number from filename
        match = re.search(r'trace_(\d+)\.txt', trace_file.name)
        if not match:
            print(f"Could not extract number from filename: {trace_file}")
            continue
        
        trace_number = match.group(1)
        
        # Extract commit portion
        first_line, commit_section, commit_duration = extract_commit_from_trace(str(trace_file))
        
        if first_line is None or commit_section is None:
            print(f"No COMMIT section found in {trace_file}")
            skipped_count += 1
            continue
        
        # Check if commit duration meets the minimum and maximum thresholds
        if commit_duration is not None:
            if commit_duration < min_duration:
                filtered_count += 1
                continue
            if commit_duration > max_duration:
                filtered_count += 1
                continue
        
        # Analyze timing patterns
        timing_analysis = analyze_commit_timing(commit_section)
        
        # Store valid commit for sorting
        valid_commits.append({
            'trace_file': str(trace_file),
            'trace_number': trace_number,
            'first_line': first_line,
            'commit_section': commit_section,
            'commit_duration': commit_duration,
            'timing_analysis': timing_analysis
        })
    
    # Sort commits by duration (slowest first)
    valid_commits.sort(key=lambda x: x['commit_duration'] or 0, reverse=True)
    
    # Create subdirectories
    query_intent_dir = output_dir / "query_intent"
    network_dir = output_dir / "network"
    raft_dir = output_dir / "raft"
    store_send_dir = output_dir / "store_send"
    replica_send_dir = output_dir / "replica_send"
    other_dir = output_dir / "other"
    query_intent_dir.mkdir(exist_ok=True)
    network_dir.mkdir(exist_ok=True)
    raft_dir.mkdir(exist_ok=True)
    store_send_dir.mkdir(exist_ok=True)
    replica_send_dir.mkdir(exist_ok=True)
    other_dir.mkdir(exist_ok=True)
    
    # Track counts for each category
    query_intent_count = 0
    network_count = 0
    raft_count = 0
    store_send_count = 0
    replica_send_count = 0
    other_count = 0
    
    # Write sorted commits with monotonically increasing prefix
    for index, commit_data in enumerate(valid_commits, 1):
        trace_number = commit_data['trace_number']
        first_line = commit_data['first_line']
        commit_section = commit_data['commit_section']
        commit_duration = commit_data['commit_duration']
        timing_analysis = commit_data['timing_analysis']
        
        # Determine category based on timing analysis
        category = "other"
        if timing_analysis and timing_analysis['is_query_intent']:
            category = "query_intent"
            query_intent_count += 1
        else:
            # Check for network operations
            network_analysis = analyze_network_timing(commit_section, network_threshold)
            if network_analysis and network_analysis['longest_client_server'] and network_analysis['longest_client_server']['latency'] > network_threshold:
                category = "network"
                network_count += 1
            elif network_analysis and network_analysis['longest_server_client'] and network_analysis['longest_server_client']['latency'] > network_threshold:
                category = "network_server_client"
                network_count += 1
            else:
                # Check for abnormal network patterns
                if network_analysis and network_analysis['abnormal_network']:
                    category = "network_abnormal"
                    network_count += 1
                else:
                    # Check for Raft operations
                    raft_analysis = analyze_raft_timing(commit_section, raft_threshold)
                    if raft_analysis and raft_analysis['longest_raft']:
                        category = "raft"
                        raft_count += 1
                    else:
                        # Check for store_send operations
                        store_send_analysis = analyze_store_send_timing(commit_section, store_send_threshold)
                        if store_send_analysis and store_send_analysis['longest_store_send']:
                            category = "store_send"
                            store_send_count += 1
                        else:
                            # Check for replica_send operations
                            replica_send_analysis = analyze_replica_send_timing(commit_section, replica_send_threshold)
                            if replica_send_analysis and replica_send_analysis['longest_replica_send']:
                                category = "replica_send"
                                replica_send_count += 1
                            else:
                                other_count += 1
        
        # Choose output directory based on category
        if category == "query_intent":
            target_dir = query_intent_dir
        elif category == "network":
            # Create node-specific subdirectory under client_server
            network_analysis = analyze_network_timing(commit_section, network_threshold)
            node_number = network_analysis['longest_client_server']['server_node']
            client_server_dir = network_dir / "client_server"
            client_server_dir.mkdir(exist_ok=True)
            node_dir = client_server_dir / str(node_number)
            node_dir.mkdir(exist_ok=True)
            target_dir = node_dir
        elif category == "network_server_client":
            # Create node-specific subdirectory under server_client
            network_analysis = analyze_network_timing(commit_section, network_threshold)
            node_number = network_analysis['longest_server_client']['server_node']
            server_client_dir = network_dir / "server_client"
            server_client_dir.mkdir(exist_ok=True)
            node_dir = server_client_dir / str(node_number)
            node_dir.mkdir(exist_ok=True)
            target_dir = node_dir
        elif category == "network_abnormal":
            # Create node-specific subdirectory under abnormal
            network_analysis = analyze_network_timing(commit_section, network_threshold)
            node_number = network_analysis['abnormal_network']['server_node']
            abnormal_dir = network_dir / "abnormal"
            abnormal_dir.mkdir(exist_ok=True)
            node_dir = abnormal_dir / str(node_number)
            node_dir.mkdir(exist_ok=True)
            target_dir = node_dir
        elif category == "raft":
            # Create node-specific subdirectory
            raft_analysis = analyze_raft_timing(commit_section, raft_threshold)
            node_number = raft_analysis['longest_raft']['node']
            node_dir = raft_dir / str(node_number)
            node_dir.mkdir(exist_ok=True)
            target_dir = node_dir
        elif category == "store_send":
            # Create node-specific subdirectory
            store_send_analysis = analyze_store_send_timing(commit_section, store_send_threshold)
            node_number = store_send_analysis['longest_store_send']['node']
            node_dir = store_send_dir / str(node_number)
            node_dir.mkdir(exist_ok=True)
            target_dir = node_dir
        elif category == "replica_send":
            # Create node-specific subdirectory
            replica_send_analysis = analyze_replica_send_timing(commit_section, replica_send_threshold)
            node_number = replica_send_analysis['longest_replica_send']['node']
            node_dir = replica_send_dir / str(node_number)
            node_dir.mkdir(exist_ok=True)
            target_dir = node_dir
        else:
            target_dir = other_dir
        
        # Create output filename with prefix
        output_filename = f"{index}_extracted_commit_{trace_number}.txt"
        output_path = target_dir / output_filename
        
        # Write the extracted commit with first line prepended
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                # Write the COMMIT duration info at the top
                if commit_duration is not None:
                    f.write(f"COMMIT ran for a total of: {commit_duration:.3f} ms\n")
                else:
                    f.write("COMMIT duration: Could not calculate\n")
                f.write('\n')  # Add a blank line for separation
                
                # Write timing analysis info
                if timing_analysis:
                    f.write(f"Longest step: {timing_analysis['longest_step_time']:.3f} ms\n")
                    f.write(f"Category: {category}\n")
                    f.write(f"Longest step details: {timing_analysis['longest_step_line']}\n")
                    
                    # Add network information if applicable
                    if category == "network":
                        network_analysis = analyze_network_timing(commit_section, network_threshold)
                        if network_analysis and network_analysis['longest_client_server']:
                            network_op = network_analysis['longest_client_server']
                            f.write(f"Client->server latency: {network_op['latency']:.3f} ms\n")
                            f.write(f"  Client node: {network_op['client_node']}\n")
                            f.write(f"  Server node: {network_op['server_node']}\n")
                    
                    # Add server-client round trip information if applicable
                    if category == "network_server_client":
                        network_analysis = analyze_network_timing(commit_section, network_threshold)
                        if network_analysis and network_analysis['longest_server_client']:
                            round_trip_op = network_analysis['longest_server_client']
                            f.write(f"Server->client latency: {round_trip_op['latency']:.3f} ms\n")
                            f.write(f"  Server node: {round_trip_op['server_node']}\n")
                            f.write(f"  Client node: {round_trip_op['client_node']}\n")
                            f.write(f"  Server response: {round_trip_op['server_response_timestamp']:.3f} ms\n")
                            f.write(f"  Client received: {round_trip_op['client_received_timestamp']:.3f} ms\n")
                    
                    # Add abnormal network information if applicable
                    if category == "network_abnormal":
                        network_analysis = analyze_network_timing(commit_section, network_threshold)
                        if network_analysis and network_analysis['abnormal_network']:
                            abnormal_op = network_analysis['abnormal_network']
                            f.write(f"Abnormal network pattern: {abnormal_op['discrepancy']:.3f} ms discrepancy\n")
                            f.write(f"  Server node: {abnormal_op['server_node']}\n")
                            f.write(f"  Client node: {abnormal_op['client_node']}\n")
                            f.write(f"  Client->server latency: {abnormal_op['client_server_latency']:.3f} ms\n")
                            f.write(f"  Server->client latency: {abnormal_op['server_client_latency']:.3f} ms\n")
                    
                    # Add Raft information if applicable
                    if category == "raft":
                        raft_analysis = analyze_raft_timing(commit_section, raft_threshold)
                        if raft_analysis and raft_analysis['longest_raft']:
                            raft_op = raft_analysis['longest_raft']
                            f.write(f"Raft operations: {raft_op['total_duration']:.3f} ms total on node {raft_op['node']}\n")
                            for op in raft_op['operations']:
                                f.write(f"  {op['type']}: {op['duration']:.3f} ms\n")
                    
                    # Add store_send information if applicable
                    if category == "store_send":
                        store_send_analysis = analyze_store_send_timing(commit_section, store_send_threshold)
                        if store_send_analysis and store_send_analysis['longest_store_send']:
                            store_send_op = store_send_analysis['longest_store_send']
                            f.write(f"Store_send operation: {store_send_op['duration']:.3f} ms on node {store_send_op['node']}\n")
                    
                    # Add replica_send information if applicable
                    if category == "replica_send":
                        replica_send_analysis = analyze_replica_send_timing(commit_section, replica_send_threshold)
                        if replica_send_analysis and replica_send_analysis['longest_replica_send']:
                            replica_send_op = replica_send_analysis['longest_replica_send']
                            f.write(f"Replica_send operation: {replica_send_op['duration']:.3f} ms on node {replica_send_op['node']}\n")
                    
                    f.write('\n')  # Add a blank line for separation
                
                # Write the first line (transaction duration info)
                f.write(first_line + '\n')
                f.write('\n')  # Add a blank line for separation
                
                # Write the commit section
                f.writelines(commit_section)
            
            processed_count += 1
            
        except Exception as e:
            print(f"Error writing {output_filename}: {e}")
            skipped_count += 1
    
    print(f"\nProcessing complete!")
    print(f"Successfully processed: {processed_count} files")
    
    # Calculate percentages
    if processed_count > 0:
        query_intent_pct = (query_intent_count / processed_count) * 100
        network_pct = (network_count / processed_count) * 100
        raft_pct = (raft_count / processed_count) * 100
        store_send_pct = (store_send_count / processed_count) * 100
        replica_send_pct = (replica_send_count / processed_count) * 100
        other_pct = (other_count / processed_count) * 100
        
        print(f"  - QueryIntent category: {query_intent_count} files ({query_intent_pct:.1f}%)")
        print(f"  - Network category: {network_count} files ({network_pct:.1f}%)")
        print(f"  - Raft category: {raft_count} files ({raft_pct:.1f}%)")
        print(f"  - Store_send category: {store_send_count} files ({store_send_pct:.1f}%)")
        print(f"  - Replica_send category: {replica_send_count} files ({replica_send_pct:.1f}%)")
        print(f"  - Other category: {other_count} files ({other_pct:.1f}%)")
    else:
        print(f"  - QueryIntent category: {query_intent_count} files")
        print(f"  - Network category: {network_count} files")
        print(f"  - Raft category: {raft_count} files")
        print(f"  - Store_send category: {store_send_count} files")
        print(f"  - Replica_send category: {replica_send_count} files")
        print(f"  - Other category: {other_count} files")
    
    print(f"Skipped: {skipped_count} files")
    print(f"Filtered out (duration < {min_duration}ms or > {max_duration}ms): {filtered_count} files")
    print(f"Network threshold: {network_threshold}ms")
    print(f"Raft threshold: {raft_threshold}ms")
    print(f"Store_send threshold: {store_send_threshold}ms")
    print(f"Replica_send threshold: {replica_send_threshold}ms")
    print(f"Output directory: {output_dir.absolute()}")
    print(f"  - QueryIntent files: {query_intent_dir.absolute()}")
    print(f"  - Network files: {network_dir.absolute()}")
    print(f"  - Raft files: {raft_dir.absolute()}")
    print(f"  - Other files: {other_dir.absolute()}")


if __name__ == "__main__":
    main() 