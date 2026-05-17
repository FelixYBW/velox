#!/usr/bin/env python3
"""
Read /tmp/velox_s3_read_trace.log into a pandas DataFrame.
This script parses the S3 read trace log and loads it into a structured DataFrame.
"""

import pandas as pd
import re
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np


def read_s3_trace_log(log_path):
    """
    Read S3 trace log file into a pandas DataFrame.
    
    Args:
        log_path: Path to the log file (default: /tmp/velox_s3_read_trace.log)
    
    Returns:
        pandas.DataFrame: Parsed log data
    """
    log_file = Path(log_path)
    
    if not log_file.exists():
        raise FileNotFoundError(f"Log file not found: {log_path}")
    
    # Read all lines from the log file
    with open(log_file, 'r') as f:
        lines = f.readlines()
    
    if not lines:
        print(f"Warning: Log file is empty: {log_path}")
        return pd.DataFrame()
    
    # Try to detect the log format by examining the first few lines
    # Common formats:
    # 1. CSV-like: timestamp,field1,field2,...
    # 2. Space/tab delimited
    # 3. JSON lines
    # 4. Custom structured format
    
    # Check if it's CSV-like
    if ',' in lines[0]:
        try:
            df = pd.read_csv(log_file)
            print(f"Successfully read {len(df)} rows as CSV format")
            return df
        except Exception as e:
            print(f"Failed to read as CSV: {e}")
    
    # Check if it's tab-delimited
    if '\t' in lines[0]:
        try:
            df = pd.read_csv(log_file, sep='\t')
            print(f"Successfully read {len(df)} rows as tab-delimited format")
            return df
        except Exception as e:
            print(f"Failed to read as tab-delimited: {e}")
    
    # Try to parse as structured log lines
    # Common pattern: [timestamp] field1=value1 field2=value2 ...
    data = []
    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
        
        # Skip HeadObject entries
        if 'HeadObject' in line:
            continue
        
        # Try to extract key-value pairs
        entry = {}
        
        # Extract timestamp before "GetObject" if present
        # Pattern: timestamp (various formats) followed by GetObject
        timestamp_before_getobject = re.search(
            r'(\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)\s+.*?GetObject',
            line
        )
        if timestamp_before_getobject:
            entry['timestamp'] = timestamp_before_getobject.group(1)
        else:
            # Try alternative timestamp pattern before GetObject
            timestamp_alt = re.search(r'(\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+.*?GetObject', line)
            if timestamp_alt:
                entry['timestamp'] = timestamp_alt.group(1)
        
        # If no timestamp before GetObject, try general timestamp patterns
        if 'timestamp' not in entry:
            timestamp_patterns = [
                r'^\[?(\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)\]?',
                r'^(\d{2}:\d{2}:\d{2}(?:\.\d+)?)',
                r'timestamp[=:](\d+)',
            ]
            
            for pattern in timestamp_patterns:
                match = re.search(pattern, line)
                if match:
                    entry['timestamp'] = match.group(1)
                    break
        
        # Extract key=value pairs
        kv_pattern = r'(\w+)=([^\s,]+)'
        matches = re.findall(kv_pattern, line)
        for key, value in matches:
            # Special handling for 'range' field with bytes=start-end format
            if key == 'range' or key == 'bytes':
                range_match = re.match(r'bytes=(\d+)-(\d+)', value)
                if range_match:
                    start_pos = int(range_match.group(1))
                    end_pos = int(range_match.group(2))
                    entry['startposition'] = start_pos
                    entry['endposition'] = end_pos
                    entry['length'] = end_pos - start_pos + 1
                    entry[key] = value  # Keep original range string
                    continue
            
            # Try to convert to numeric if possible
            try:
                if '.' in value:
                    entry[key] = float(value)
                else:
                    entry[key] = int(value)
            except ValueError:
                entry[key] = value
        
        # Also check for standalone bytes=start-end pattern not in key=value format
        if 'startposition' not in entry:
            bytes_pattern = r'bytes=(\d+)-(\d+)'
            bytes_match = re.search(bytes_pattern, line)
            if bytes_match:
                start_pos = int(bytes_match.group(1))
                end_pos = int(bytes_match.group(2))
                entry['startposition'] = start_pos
                entry['endposition'] = end_pos
                entry['length'] = end_pos - start_pos + 1
                entry['range'] = f"bytes={start_pos}-{end_pos}"
        
        # If no key-value pairs found, store the entire line
        if not entry:
            entry['line'] = line
            entry['line_number'] = line_num
        
        data.append(entry)
    
    if data:
        df = pd.DataFrame(data)
        print(f"Successfully parsed {len(df)} rows from structured log format")
        print(f"Columns: {list(df.columns)}")
        return df
    
    # If all parsing attempts fail, return raw lines as DataFrame
    print("Warning: Could not parse structured data, returning raw lines")
    df = pd.DataFrame({'line': [line.strip() for line in lines if line.strip()]})
    return df


def analyze_dataframe(df):
    """
    Print basic analysis of the DataFrame.
    
    Args:
        df: pandas DataFrame to analyze
    """
    print("\n" + "="*60)
    print("DataFrame Analysis")
    print("="*60)
    print(f"\nShape: {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nData types:\n{df.dtypes}")
    print(f"\nFirst few rows:\n{df.head()}")
    
    if len(df) > 0:
        print(f"\nBasic statistics:\n{df.describe()}")
        
        # Check for missing values
        missing = df.isnull().sum()
        if missing.any():
            print(f"\nMissing values:\n{missing[missing > 0]}")


def plot_length_histogram(df, output_file='length_histogram.png'):
    """
    Create a histogram chart based on the 'length' column in the DataFrame.
    
    Args:
        df: pandas DataFrame containing 'length' column
        output_file: Path to save the histogram image (default: length_histogram.png)
    """
    if df is None or df.empty:
        print("Cannot create histogram: DataFrame is empty")
        return
    
    if 'length' not in df.columns:
        print("Cannot create histogram: 'length' column not found in DataFrame")
        print(f"Available columns: {list(df.columns)}")
        return
    
    # Filter out any NaN values
    lengths = df['length'].dropna()
    
    if len(lengths) == 0:
        print("Cannot create histogram: No valid length data found")
        return
    
    print(f"\nCreating histogram for {len(lengths)} length values...")
    print(f"Length statistics:")
    print(f"  Min: {lengths.min():,} bytes")
    print(f"  Max: {lengths.max():,} bytes")
    print(f"  Mean: {lengths.mean():,.2f} bytes")
    print(f"  Median: {lengths.median():,} bytes")
    
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Calculate appropriate number of bins
    # Use Sturges' rule or sqrt rule, whichever gives more bins
    n_bins = max(int(np.ceil(np.log2(len(lengths)) + 1)), int(np.ceil(np.sqrt(len(lengths)))))
    n_bins = min(n_bins, 50)  # Cap at 50 bins for readability
    
    # Create histogram
    counts, bins, patches = ax.hist(lengths, bins=n_bins, edgecolor='black', alpha=0.7, color='steelblue')
    
    # Add labels and title
    ax.set_xlabel('Length (bytes)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Frequency', fontsize=12, fontweight='bold')
    ax.set_title('Histogram of S3 Read Request Lengths', fontsize=14, fontweight='bold', pad=20)
    
    # Format x-axis to show readable byte values
    ax.ticklabel_format(style='plain', axis='x')
    
    # Add grid for better readability
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    
    # Add statistics text box
    stats_text = f'Total Requests: {len(lengths):,}\n'
    stats_text += f'Min: {lengths.min():,} bytes\n'
    stats_text += f'Max: {lengths.max():,} bytes\n'
    stats_text += f'Mean: {lengths.mean():,.0f} bytes\n'
    stats_text += f'Median: {lengths.median():,} bytes'
    
    ax.text(0.98, 0.97, stats_text,
            transform=ax.transAxes,
            fontsize=10,
            verticalalignment='top',
            horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the figure
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nHistogram saved to: {output_file}")
    
    # Also display if in interactive mode
    try:
        plt.show()
    except:
        pass  # Non-interactive environment
    
    plt.close()


def main():
    """Main function to read and display the S3 trace log."""
    log_path = '/velox/_build/release/velox/connectors/hive/storage_adapters/s3fs/velox_s3_read_trace_q10.log'
    
    print(f"Reading S3 trace log from: {log_path}")
    print("-" * 60)
    
    try:
        df = read_s3_trace_log(log_path)
        
        if df is not None and not df.empty:
            analyze_dataframe(df)
            
            # Return the DataFrame for further use
            return df
        else:
            print("No data was loaded from the log file.")
            return None
            
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("\nPlease ensure the log file exists at the specified path.")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == '__main__':
    df = main()
    
    # The DataFrame is now available as 'df' for further analysis
    if df is not None:
        print("\n" + "="*60)
        print("DataFrame loaded successfully!")
        print("You can now use 'df' variable for further analysis.")
        print("="*60)
        
        # Create histogram if length data is available
        if 'length' in df.columns:
            print("\n" + "="*60)
            print("Creating histogram chart...")
            print("="*60)
            plot_length_histogram(df)
        else:
            print("\nNote: 'length' column not found. Histogram not created.")

# Made with Bob
