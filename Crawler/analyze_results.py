#!/usr/bin/env python3

import os
import pandas as pd
import matplotlib.pyplot as plt
import glob
import re
import argparse

def analyze_performance_results(results_dir="performance_results"):
    """Analyze performance test results and create summary visualizations"""
    print("Analyzing performance results...")
    
    # Find all CSV result files
    csv_files = glob.glob(f"{results_dir}/performance_data_*_workers.csv")
    if not csv_files:
        print(f"No CSV files found in {results_dir}")
        return
        
    # Parse worker count from filenames
    pattern = re.compile(r'performance_data_(\d+)_workers\.csv')
    
    # Load all data frames and their worker counts
    dfs = []
    worker_counts = []
    
    for csv_file in csv_files:
        match = pattern.search(csv_file)
        if match:
            worker_count = int(match.group(1))
            df = pd.read_csv(csv_file)
            df['worker_count'] = worker_count
            dfs.append(df)
            worker_counts.append(worker_count)
    
    # Combine all data
    if not dfs:
        print("No valid data files found")
        return
        
    # Calculate max throughput for each worker count
    throughputs = []
    for i, df in enumerate(dfs):
        if 'Throughput (pages/sec)' in df.columns:
            max_throughput = df['Throughput (pages/sec)'].max()
            throughputs.append((worker_counts[i], max_throughput))
    
    # Sort by worker count
    throughputs.sort(key=lambda x: x[0])
    
    # Create summary data frame
    summary_df = pd.DataFrame(throughputs, columns=['Workers', 'Max Throughput'])
    
    # Create enhanced visualization
    plt.figure(figsize=(12, 8))
    
    # Bar chart for max throughput
    plt.subplot(2, 2, 1)
    plt.bar(summary_df['Workers'], summary_df['Max Throughput'], color='skyblue')
    plt.title('Maximum Throughput by Worker Count')
    plt.xlabel('Number of Workers')
    plt.ylabel('Pages per Second')
    plt.grid(True, axis='y')
    
    # Line chart for throughput scaling
    plt.subplot(2, 2, 2)
    plt.plot(summary_df['Workers'], summary_df['Max Throughput'], 'o-', color='navy')
    plt.title('Throughput Scaling')
    plt.xlabel('Number of Workers')
    plt.ylabel('Pages per Second')
    plt.grid(True)
    
    # Calculate efficiency (throughput per worker)
    summary_df['Efficiency'] = summary_df['Max Throughput'] / summary_df['Workers']
    
    # Line chart for efficiency
    plt.subplot(2, 2, 3)
    plt.plot(summary_df['Workers'], summary_df['Efficiency'], 'o-', color='green')
    plt.title('Crawler Efficiency (Throughput per Worker)')
    plt.xlabel('Number of Workers')
    plt.ylabel('Pages per Second per Worker')
    plt.grid(True)
    
    # Calculate speedup relative to single worker
    if len(summary_df) > 0 and summary_df['Workers'].min() == 1:
        single_worker_throughput = summary_df.loc[summary_df['Workers'] == 1, 'Max Throughput'].values[0]
        summary_df['Speedup'] = summary_df['Max Throughput'] / single_worker_throughput
        
        # Line chart for speedup
        plt.subplot(2, 2, 4)
        plt.plot(summary_df['Workers'], summary_df['Speedup'], 'o-', color='red')
        plt.plot(summary_df['Workers'], summary_df['Workers'], '--', color='gray', alpha=0.7)
        plt.title('Speedup vs. Linear Scaling')
        plt.xlabel('Number of Workers')
        plt.ylabel('Speedup Factor')
        plt.grid(True)
    
    plt.tight_layout()
    plt.savefig(f"{results_dir}/enhanced_scalability_analysis.png")
    
    # Save the summary to CSV
    summary_df.to_csv(f"{results_dir}/performance_summary.csv", index=False)
    
    print(f"Analysis complete. Results saved to {results_dir}/enhanced_scalability_analysis.png")
    print(f"Summary data saved to {results_dir}/performance_summary.csv")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze distributed crawler performance results")
    parser.add_argument("--results-dir", default="performance_results", 
                        help="Directory containing performance results (default: performance_results)")
    
    args = parser.parse_args()
    analyze_performance_results(args.results_dir)
