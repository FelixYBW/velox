#!/usr/bin/env python3
"""
Parse network.txt and run.log to visualize RxKB/s during benchmark runs.
"""

import re
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def parse_run_log(filename):
    """Parse run.log to extract thread counts and their start/end times."""
    runs = []
    with open(filename, 'r') as f:
        current_run = {}
        for line in f:
            # Match "Running with num_threads=X"
            match = re.search(r'Running with num_threads=(\d+)', line)
            if match:
                current_run = {'threads': int(match.group(1))}
            
            # Match "Start time: HH:MM:SS"
            match = re.search(r'Start time: (\d{2}:\d{2}:\d{2})', line)
            if match and current_run:
                current_run['start_time'] = match.group(1)
            
            # Match "End time: HH:MM:SS"
            match = re.search(r'End time: (\d{2}:\d{2}:\d{2})', line)
            if match and current_run:
                current_run['end_time'] = match.group(1)
                runs.append(current_run.copy())
                current_run = {}
    
    return runs

def parse_network_txt(filename):
    """Parse network.txt to extract timestamp and RxKB/s for enp39s0 interface."""
    network_data = []
    with open(filename, 'r') as f:
        for line in f:
            # Match lines with timestamp and interface data
            # Format: HH:MM:SS        IFACE   rxpck/s   txpck/s    rxkB/s    txkB/s ...
            parts = line.split()
            if len(parts) >= 5 and parts[1] == 'enp39s0':
                try:
                    time_str = parts[0]
                    rxkb_s = float(parts[4])
                    network_data.append({
                        'time': time_str,
                        'rxkb_s': rxkb_s
                    })
                except (ValueError, IndexError):
                    continue
    
    return network_data

def time_to_datetime(time_str, base_date='2026-05-16'):
    """Convert HH:MM:SS string to datetime object."""
    return datetime.strptime(f"{base_date} {time_str}", "%Y-%m-%d %H:%M:%S")

def filter_network_data_by_time(network_data, start_time, end_time):
    """Filter network data between start and end times."""
    filtered = []
    for entry in network_data:
        entry_time = time_to_datetime(entry['time'])
        if start_time <= entry_time <= end_time:
            filtered.append({
                'datetime': entry_time,
                'rxkb_s': entry['rxkb_s']
            })
    return filtered

def main():
    # Parse the log files
    print("Parsing run.log...")
    runs = parse_run_log('run.log')
    print(f"Found {len(runs)} benchmark runs")
    
    print("\nParsing network.txt...")
    network_data = parse_network_txt('network.txt')
    print(f"Found {len(network_data)} network data points")
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Plot each run's network data
    for run in runs:
        threads = run['threads']
        start_time = time_to_datetime(run['start_time'])
        end_time = time_to_datetime(run['end_time'])
        
        # Filter network data for this run
        run_network = filter_network_data_by_time(network_data, start_time, end_time)
        
        if run_network:
            times = [entry['datetime'] for entry in run_network]
            rxkb_s = [entry['rxkb_s'] for entry in run_network]
            
            # Plot with label
            ax.plot(times, rxkb_s, marker='o', linestyle='-', linewidth=2, 
                   markersize=4, label=f'{threads} threads', alpha=0.7)
            
            print(f"Threads={threads}: {len(run_network)} data points, "
                  f"avg RxKB/s={sum(rxkb_s)/len(rxkb_s):.2f}, "
                  f"max RxKB/s={max(rxkb_s):.2f}")
    
    # Format the plot
    ax.set_xlabel('Time', fontsize=12, fontweight='bold')
    ax.set_ylabel('RxKB/s (Receive Kilobytes per Second)', fontsize=12, fontweight='bold')
    ax.set_title('Network Receive Rate (RxKB/s) During S3 Read Benchmark\nAcross Different Thread Counts', 
                 fontsize=14, fontweight='bold', pad=20)
    
    # Format x-axis to show time nicely
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
    plt.xticks(rotation=45, ha='right')
    
    # Add grid for better readability
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # Add legend
    ax.legend(loc='upper left', fontsize=10, framealpha=0.9)
    
    # Tight layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the plot
    output_file = 'network_rxkbs_analysis.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nChart saved to: {output_file}")
    
    # Show the plot
    plt.show()

if __name__ == '__main__':
    main()

# Made with Bob
