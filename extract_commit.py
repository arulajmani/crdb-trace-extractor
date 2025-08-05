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


def main():
    """Main function to process all trace files."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract COMMIT portions from trace files')
    parser.add_argument('--min-duration', type=float, default=50.0,
                       help='Minimum COMMIT duration in milliseconds (default: 50ms)')
    args = parser.parse_args()
    
    min_duration = args.min_duration
    
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
        
        # Store valid commit for sorting
        valid_commits.append({
            'trace_file': trace_file,
            'trace_number': trace_number,
            'first_line': first_line,
            'commit_section': commit_section,
            'commit_duration': commit_duration
        })
    
    # Sort commits by duration (slowest first)
    valid_commits.sort(key=lambda x: x['commit_duration'] or 0, reverse=True)
    
    # Write sorted commits with monotonically increasing prefix
    for index, commit_data in enumerate(valid_commits, 1):
        trace_number = commit_data['trace_number']
        first_line = commit_data['first_line']
        commit_section = commit_data['commit_section']
        commit_duration = commit_data['commit_duration']
        
        # Create output filename with prefix
        output_filename = f"{index}_extracted_commit_{trace_number}.txt"
        output_path = output_dir / output_filename
        
        # Write the extracted commit with first line prepended
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                # Write the COMMIT duration info at the top
                if commit_duration is not None:
                    f.write(f"COMMIT ran for a total of: {commit_duration:.3f} ms\n")
                else:
                    f.write("COMMIT duration: Could not calculate\n")
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
    print(f"Skipped: {skipped_count} files")
    print(f"Filtered out (duration < {min_duration}ms): {filtered_count} files")
    print(f"Output directory: {output_dir.absolute()}")


if __name__ == "__main__":
    main() 