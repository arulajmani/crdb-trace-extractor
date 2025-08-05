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


def analyze_network_timing(commit_section, network_threshold):
    """
    Analyze network timing patterns in the COMMIT section.
    
    Args:
        commit_section (list): List of lines containing the COMMIT section
        network_threshold (float): Threshold in milliseconds for network operations
        
    Returns:
        dict: Network analysis results with timing information and node info
    """
    try:
        if not commit_section:
            return None
        
        # Look for client-server batch operation patterns
        client_operations = []
        server_operations = []
        
        for line in commit_section:
            # Look for client operations
            if ('=== operation:/cockroach.roachpb.Internal/Batch' in line and 
                'span.kind:‹client›' in line):
                # Extract timestamp and node info
                match = re.search(r'^\s*(\d+\.\d+)ms', line)
                if match:
                    timestamp = float(match.group(1))
                    # Extract node number
                    node_match = re.search(r'node:‹(\d+)›', line)
                    node = node_match.group(1) if node_match else None
                    client_operations.append({
                        'timestamp': timestamp,
                        'node': node,
                        'line': line.strip()
                    })
            
            # Look for server operations
            elif ('=== operation:/cockroach.roachpb.Internal/Batch' in line and 
                  'span.kind:‹server›' in line):
                # Extract timestamp and duration
                match = re.search(r'^\s*(\d+\.\d+)ms\s+(\d+\.\d+)ms', line)
                if match:
                    timestamp = float(match.group(1))
                    duration = float(match.group(2))
                    # Extract node number
                    node_match = re.search(r'node:‹(\d+)›', line)
                    node = node_match.group(1) if node_match else None
                    server_operations.append({
                        'timestamp': timestamp,
                        'duration': duration,
                        'node': node,
                        'line': line.strip()
                    })
        
        # Find the longest network operation
        longest_network = None
        for server_op in server_operations:
            if server_op['duration'] > network_threshold:
                if longest_network is None or server_op['duration'] > longest_network['duration']:
                    longest_network = server_op
        
        return {
            'longest_network': longest_network,
            'client_operations': client_operations,
            'server_operations': server_operations
        }
        
    except Exception as e:
        print(f"Error analyzing network timing: {e}")
        return None


def main():
    """Main function to process all trace files."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract COMMIT portions from trace files')
    parser.add_argument('--min-duration', type=float, default=50.0,
                       help='Minimum COMMIT duration in milliseconds (default: 50ms)')
    parser.add_argument('--network-threshold', type=float, default=50.0,
                       help='Network threshold in milliseconds for categorizing network operations (default: 50ms)')
    args = parser.parse_args()
    
    min_duration = args.min_duration
    network_threshold = args.network_threshold
    
    # Create output directory
    output_dir = Path("extracted_commits")
    output_dir.mkdir(exist_ok=True)
    
    # Get all trace files
    trace_files = glob.glob("traces/trace_*.txt")
    trace_files.sort()  # Sort to ensure consistent ordering
    
    print(f"Found {len(trace_files)} trace files to process...")
    print(f"Minimum COMMIT duration filter: {min_duration}ms")
    
    processed_count = 0
    skipped_count = 0
    filtered_count = 0
    
    # Collect all valid commits with their durations for sorting
    valid_commits = []
    
    for trace_file in trace_files:
        # Extract trace number from filename
        match = re.search(r'trace_(\d+)\.txt', trace_file)
        if not match:
            print(f"Could not extract number from filename: {trace_file}")
            continue
        
        trace_number = match.group(1)
        
        # Extract commit portion
        first_line, commit_section, commit_duration = extract_commit_from_trace(trace_file)
        
        if first_line is None or commit_section is None:
            print(f"No COMMIT section found in {trace_file}")
            skipped_count += 1
            continue
        
        # Check if commit duration meets the minimum threshold
        if commit_duration is not None and commit_duration < min_duration:
            filtered_count += 1
            continue
        
        # Analyze timing patterns
        timing_analysis = analyze_commit_timing(commit_section)
        
        # Store valid commit for sorting
        valid_commits.append({
            'trace_file': trace_file,
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
    other_dir = output_dir / "other"
    query_intent_dir.mkdir(exist_ok=True)
    network_dir.mkdir(exist_ok=True)
    other_dir.mkdir(exist_ok=True)
    
    # Track counts for each category
    query_intent_count = 0
    network_count = 0
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
            if network_analysis and network_analysis['longest_network']:
                category = "network"
                network_count += 1
            else:
                other_count += 1
        
        # Choose output directory based on category
        if category == "query_intent":
            target_dir = query_intent_dir
        elif category == "network":
            # Create node-specific subdirectory
            network_analysis = analyze_network_timing(commit_section, network_threshold)
            node_number = network_analysis['longest_network']['node']
            node_dir = network_dir / str(node_number)
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
                        if network_analysis and network_analysis['longest_network']:
                            network_op = network_analysis['longest_network']
                            f.write(f"Network operation: {network_op['duration']:.3f} ms on node {network_op['node']}\n")
                    
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
    print(f"  - QueryIntent category: {query_intent_count} files")
    print(f"  - Network category: {network_count} files")
    print(f"  - Other category: {other_count} files")
    print(f"Skipped: {skipped_count} files")
    print(f"Filtered out (duration < {min_duration}ms): {filtered_count} files")
    print(f"Network threshold: {network_threshold}ms")
    print(f"Output directory: {output_dir.absolute()}")
    print(f"  - QueryIntent files: {query_intent_dir.absolute()}")
    print(f"  - Network files: {network_dir.absolute()}")
    print(f"  - Other files: {other_dir.absolute()}")


if __name__ == "__main__":
    main() 