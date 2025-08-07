import re
import os
import shutil
import argparse
import glob

def find_log_files(debug_zip_path: str) -> list:
    """
    Find all log files in a debug zip directory structure.
    Returns a list of (node_id, log_file_path) tuples.
    """
    log_files = []
    
    # Look for nodes directory
    nodes_dir = os.path.join(debug_zip_path, "nodes")
    if not os.path.exists(nodes_dir):
        print(f"Warning: No 'nodes' directory found in {debug_zip_path}")
        return log_files
    
    # Find all node directories
    for node_dir in os.listdir(nodes_dir):
        node_path = os.path.join(nodes_dir, node_dir)
        if os.path.isdir(node_path):
            # Look for logs directory in this node
            logs_dir = os.path.join(node_path, "logs")
            if os.path.exists(logs_dir):
                # Find all .log files in the logs directory
                for log_file in os.listdir(logs_dir):
                    if log_file.endswith('.log'):
                        log_file_path = os.path.join(logs_dir, log_file)
                        log_files.append((node_dir, log_file_path))
    
    return log_files

def extract_slow_traces_from_file(input_path: str, min_threshold_ms: float = 200.0, filter_pattern: str = None) -> list:
    """
    Extracts slow traces from a single log file.
    Returns a list of trace dictionaries with 'duration', 'lines', and 'source_file' keys.
    If filter_pattern is provided, only includes traces that contain the pattern.
    """
    threshold_re = re.compile(r"SQL txn took .*exceeding threshold of .*:")
    # Pattern to extract duration from "SQL txn took Xms" or "SQL txn took X.Yms"
    duration_re = re.compile(r"SQL txn took ([\d.]+)ms")
    # Pattern to detect start of a new log entry: node> I250804 time file.go
    # NB: Hard coded to 2025-08-04.
    new_log_entry_re = re.compile(r'^[^>]+> I250804 [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6} [0-9]+ [0-9]+@[^:]+\.go:')

    traces = []
    current_trace_lines = []
    current_trace_duration = 0.0
    
    # Compile filter pattern if provided
    filter_re = None
    if filter_pattern:
        try:
            filter_re = re.compile(filter_pattern)
        except re.error as e:
            print(f"Warning: Invalid filter pattern '{filter_pattern}': {e}")
            return traces

    try:
        with open(input_path, "r", encoding='utf-8', errors='ignore') as infile:
            lines = infile.readlines()
    except Exception as e:
        print(f"Warning: Could not read {input_path}: {e}")
        return traces
        
    capturing = False
    current_trace_duration = 0.0
    for i, line in enumerate(lines):
        if threshold_re.search(line):
            # Extract duration from the threshold line
            duration_match = duration_re.search(line)
            if duration_match:
                new_trace_duration = float(duration_match.group(1))
            else:
                new_trace_duration = 0.0
            
            # If we were already capturing, save the previous trace if it meets threshold and filter
            if capturing and current_trace_lines and current_trace_duration >= min_threshold_ms:
                # Check filter if provided
                if filter_re:
                    trace_text = ''.join(current_trace_lines)
                    if not filter_re.search(trace_text):
                        # Skip this trace if it doesn't match the filter
                        pass
                    else:
                        traces.append({
                            'duration': current_trace_duration,
                            'lines': current_trace_lines.copy(),
                            'source_file': input_path
                        })
                else:
                    traces.append({
                        'duration': current_trace_duration,
                        'lines': current_trace_lines.copy(),
                        'source_file': input_path
                    })
            
            # Start capturing this new trace
            capturing = True
            current_trace_lines = [line]
            current_trace_duration = new_trace_duration

        elif capturing:
            current_trace_lines.append(line)

            # Check if the next line is a new log entry that doesn't contain the threshold
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                if new_log_entry_re.search(next_line) and not threshold_re.search(next_line):
                    # Save the current trace if it meets threshold and filter
                    if current_trace_duration >= min_threshold_ms:
                        # Check filter if provided
                        if filter_re:
                            trace_text = ''.join(current_trace_lines)
                            if not filter_re.search(trace_text):
                                # Skip this trace if it doesn't match the filter
                                pass
                            else:
                                traces.append({
                                    'duration': current_trace_duration,
                                    'lines': current_trace_lines.copy(),
                                    'source_file': input_path
                                })
                        else:
                            traces.append({
                                'duration': current_trace_duration,
                                'lines': current_trace_lines.copy(),
                                'source_file': input_path
                            })
                    
                    capturing = False
                    current_trace_lines = []

    # Don't forget to save the last trace if we were still capturing and it meets threshold and filter
    if capturing and current_trace_lines and current_trace_duration >= min_threshold_ms:
        # Check filter if provided
        if filter_re:
            trace_text = ''.join(current_trace_lines)
            if not filter_re.search(trace_text):
                # Skip this trace if it doesn't match the filter
                pass
            else:
                traces.append({
                    'duration': current_trace_duration,
                    'lines': current_trace_lines.copy(),
                    'source_file': input_path
                })
        else:
            traces.append({
                'duration': current_trace_duration,
                'lines': current_trace_lines.copy(),
                'source_file': input_path
            })

    return traces

def extract_slow_traces_from_debug_zip(debug_zip_path: str, output_dir: str, min_threshold_ms: float = 200.0, filter_pattern: str = None) -> int:
    """
    Reads all log files from a debug zip directory, extracts every trace that begins with
    a "SQL txn took … exceeding threshold" line and includes all subsequent
    indented lines, and writes each trace to a separate numbered file in output_dir.
    Only includes traces that exceed the specified threshold in milliseconds.
    """
    # Clear and recreate the output directory
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    # Find all log files
    log_files = find_log_files(debug_zip_path)
    if not log_files:
        print(f"No log files found in {debug_zip_path}")
        return 0

    print(f"Found {len(log_files)} log files to process...")
    
    all_traces = []
    files_with_zero_traces = 0
    
    # Process each log file
    for node_id, log_file_path in log_files:
        traces = extract_slow_traces_from_file(log_file_path, min_threshold_ms, filter_pattern)
        if traces:
            print(f"Node {node_id}: {os.path.basename(log_file_path)} - found {len(traces)} traces >= {min_threshold_ms}ms")
            all_traces.extend(traces)
        else:
            files_with_zero_traces += 1
    
    # Sort traces by duration (slowest first)
    all_traces.sort(key=lambda x: x['duration'], reverse=True)
    
    # Write traces to files
    trace_count = 0
    for i, trace in enumerate(all_traces):
        trace_count += 1
        # Use monotonically increasing integer for filename
        trace_filename = os.path.join(output_dir, f"trace_{trace_count:04d}.txt")
        with open(trace_filename, "w") as trace_file:
            trace_file.writelines(trace['lines'])
    
    if trace_count > 0:
        print(f"Extracted {trace_count} traces (>= {min_threshold_ms}ms) to {output_dir}/ directory")
    
    if files_with_zero_traces > 0:
        print(f"{files_with_zero_traces} files: found with zero traces")
    
    return trace_count

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract slow SQL transaction traces from debug zip files")
    parser.add_argument("--debug-zip", "-d", default="~/Downloads/debug 4", 
                       help="Path to debug zip directory (default: ~/Downloads/debug 4)")
    parser.add_argument("--output", "-o", default=None, 
                       help="Output directory for traces (default: bin/<debug_zip_name>/traces)")
    parser.add_argument("--threshold", "-t", type=float, default=200.0,
                       help="Minimum threshold in milliseconds for traces to include (default: 200.0)")
    parser.add_argument("--filter", "-f", default="portal resolved to.*COMMIT",
                       help="Regex pattern to filter traces (default: 'portal resolved to.*COMMIT')")
    
    args = parser.parse_args()
    
    # Expand user path if needed
    debug_zip_path = os.path.expanduser(args.debug_zip)
    
    if not os.path.exists(debug_zip_path):
        print(f"Error: Debug zip directory '{debug_zip_path}' does not exist")
        exit(1)
    
    # Determine output directory
    if args.output:
        output_dir = args.output
    else:
        # Extract debug zip name from path
        debug_zip_name = os.path.basename(debug_zip_path)
        output_dir = os.path.join("bin", debug_zip_name, "traces")
    
    # Create the output directory structure
    os.makedirs(output_dir, exist_ok=True)
    
    num_traces = extract_slow_traces_from_debug_zip(debug_zip_path, output_dir, args.threshold, args.filter)
    if num_traces == 0:
        print(f"No traces found >= {args.threshold}ms")
    else:
        print("Done!")