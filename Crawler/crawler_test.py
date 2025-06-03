#!/usr/bin/env python3
"""
Distributed Crawler Performance Testing Script

This script tests the performance of the distributed crawler with varying numbers of workers.
It collects metrics on throughput, memory usage, and fault tolerance.
"""

import asyncio
import traceback
import argparse
import time
import subprocess
import os
import signal
import json
import redis
from tabulate import tabulate
import pandas as pd
import matplotlib.pyplot as plt
from typing import List, Dict, Any

# Configure Redis client
redis_client = None

async def run_performance_test(
    num_workers: int,
    max_pages: int,
    seed_urls: List[str],
    redis_host: str = "localhost",
    redis_port: int = 6379,
    test_duration: int = 300,  # Default test duration: 5 minutes
    fault_tolerance: bool = False
):
    """Run a performance test with the specified number of workers"""
    global redis_client
    
    print(f"\n{'='*70}")
    print(f"Starting performance test with {num_workers} workers and {max_pages} page limit")
    print(f"{'='*70}")
    
    # Initialize Redis client
    redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    
    # Reset crawler state
    reset_crawler_state()
    
    # Start worker processes
    worker_processes = []
    
    for i in range(num_workers):
        worker_id = f"test-worker-{i+1}"
        cmd = [
            "python3", "crawler.py",
            "--mode", "worker",
            "--redis-host", redis_host,
            "--redis-port", str(redis_port),
            "--worker-id", worker_id,
            "--max-depth", "3",
            "--rate-limit", "0.2",  # Slightly faster rate limit for testing
            "--allowed-domains", "en.wikipedia.org",
            "--max-page-limit", str(max_pages)
        ]
        
        # Only the first worker gets seed URLs
        if i == 0:
            cmd.extend(["--seed-urls"] + seed_urls)
        
        # Start the worker process
        process = subprocess.Popen(cmd)
        worker_processes.append((worker_id, process))
        print(f"Started worker {worker_id} (PID: {process.pid})")
        
        # Short delay between starting workers
        await asyncio.sleep(1)
    
    # If testing fault tolerance, kill a random worker after 30 seconds
    if fault_tolerance and num_workers > 1:
        await asyncio.sleep(30)
        # Kill the second worker (index 1)
        worker_id, process = worker_processes[1]
        print(f"\n[FAULT TEST] Killing worker {worker_id} (PID: {process.pid})")
        try:
            os.kill(process.pid, signal.SIGTERM)
            print(f"Worker {worker_id} terminated for fault tolerance testing")
        except Exception as e:
            print(f"Error killing worker: {str(e)}")
    
    # Collect metrics while workers are running
    start_time = time.time()
    test_running = True
    
    # Create arrays to store time-series metrics
    timestamps = []
    pages_crawled = []
    queue_sizes = []
    active_workers = []
    throughputs = []
    
    try:
        while test_running:
            # Get current metrics
            metrics = get_crawler_metrics()
            
            # Print current status
            print_status(metrics)
            
            # Store metrics for plotting
            current_time = time.time() - start_time
            timestamps.append(current_time)
            pages_crawled.append(metrics["total_pages_crawled"])
            queue_sizes.append(metrics["queue_size"])
            active_workers.append(metrics["active_worker_count"])
            
            # Calculate and store throughput
            if len(timestamps) > 1:
                time_diff = timestamps[-1] - timestamps[-2]
                pages_diff = pages_crawled[-1] - pages_crawled[-2]
                if time_diff > 0:
                    throughputs.append(pages_diff / time_diff)
                else:
                    throughputs.append(0)
            else:
                throughputs.append(0)
            
            # Check if we've reached the page limit
            if metrics["total_pages_crawled"] >= max_pages:
                print(f"\nReached page limit of {max_pages}. Ending test.")
                test_running = False
            
            # Check if test duration has elapsed
            if time.time() - start_time >= test_duration:
                print(f"\nTest duration of {test_duration} seconds elapsed. Ending test.")
                test_running = False
            
            # Short delay before collecting metrics again
            await asyncio.sleep(5)
            
    except KeyboardInterrupt:
        print("\nTest interrupted by user.")
    finally:
        # Calculate final metrics
        end_time = time.time()
        total_time = end_time - start_time
        final_metrics = get_crawler_metrics()
        
        # Calculate aggregate metrics
        pages_per_second = final_metrics["total_pages_crawled"] / total_time if total_time > 0 else 0
        pages_per_worker = final_metrics["total_pages_crawled"] / num_workers
        
        # Print summary
        print("\n" + "="*70)
        print(f"Performance Test Summary ({num_workers} workers)")
        print("="*70)
        print(f"Total runtime: {total_time:.2f} seconds")
        print(f"Total pages crawled: {final_metrics['total_pages_crawled']}")
        print(f"Pages per second: {pages_per_second:.2f}")
        print(f"Pages per worker: {pages_per_worker:.2f}")
        print(f"URLs found: {final_metrics['total_urls_found']}")
        print(f"Errors: {final_metrics['total_errors']}")
        print(f"Final queue size: {final_metrics['queue_size']}")
        
        # Generate performance graphs
        generate_performance_graphs(
            num_workers, 
            timestamps, 
            pages_crawled, 
            queue_sizes, 
            active_workers, 
            throughputs
        )
        
        # Terminate all worker processes
        for worker_id, process in worker_processes:
            try:
                process.terminate()
                process.wait(timeout=5)
                print(f"Terminated worker {worker_id}")
            except:
                try:
                    # Force kill if graceful termination fails
                    os.kill(process.pid, signal.SIGKILL)
                    print(f"Force killed worker {worker_id}")
                except:
                    pass
        
        # Return metrics for comparison
        return {
            "num_workers": num_workers,
            "pages_crawled": final_metrics["total_pages_crawled"],
            "runtime": total_time,
            "pages_per_second": pages_per_second,
            "pages_per_worker": pages_per_worker,
            "errors": final_metrics["total_errors"]
        }

def reset_crawler_state():
    """Reset the crawler state in Redis"""
    keys_to_delete = [
        "crawler:queue",
        "crawler:visited",
        "crawler:stats",
        "crawler:global_page_count"
    ]
    
    for key in keys_to_delete:
        redis_client.delete(key)
        
    # Delete all worker heartbeats
    for key in redis_client.scan_iter("crawler:worker_heartbeat:*"):
        redis_client.delete(key)
        
    # Clear active workers
    redis_client.delete("crawler:active_workers")
    
    print("Crawler state reset")

def get_crawler_metrics():
    """Get current crawler metrics from Redis"""
    # Get queue stats
    queue_size = redis_client.zcard("crawler:queue")
    visited_size = redis_client.scard("crawler:visited")
    
    # Get active workers count
    active_workers = 0
    for key in redis_client.scan_iter("crawler:worker_heartbeat:*"):
        if redis_client.exists(key):
            active_workers += 1
    
    # Get aggregate stats from all workers
    all_stats = redis_client.hgetall("crawler:stats")
    
    total_pages_crawled = 0
    total_urls_found = 0
    total_errors = 0
    worker_stats = {}
    
    for worker_id, stats_json in all_stats.items():
        try:
            stats = json.loads(stats_json)
            worker_stats[worker_id] = stats
            
            total_pages_crawled += stats.get("pages_crawled", 0)
            total_urls_found += stats.get("urls_found", 0)
            total_errors += stats.get("errors", 0)
        except:
            pass
    
    return {
        "queue_size": queue_size,
        "visited_urls": visited_size,
        "active_worker_count": active_workers,
        "total_pages_crawled": total_pages_crawled,
        "total_urls_found": total_urls_found,
        "total_errors": total_errors,
        "worker_stats": worker_stats
    }

def print_status(metrics):
    """Print current crawler status"""
    # Clear terminal for cleaner output
    # os.system('cls' if os.name == 'nt' else 'clear')
    
    print("\n=== Distributed Crawler Status ===")
    print(f"Active Workers: {metrics['active_worker_count']}")
    print(f"Pages Crawled: {metrics['total_pages_crawled']}")
    print(f"URLs Found: {metrics['total_urls_found']}")
    print(f"Queue Size: {metrics['queue_size']}")
    print(f"Visited URLs: {metrics['visited_urls']}")
    print(f"Errors: {metrics['total_errors']}")
    
    # Print per-worker stats if available
    if metrics['worker_stats']:
        print("\n--- Worker Statistics ---")
        worker_table = []
        for worker_id, stats in metrics['worker_stats'].items():
            # Calculate worker throughput if data available
            throughput = "N/A"
            if "throughput" in stats and stats["throughput"]:
                latest_throughput = stats["throughput"][-1]["pages_per_second"]
                throughput = f"{latest_throughput:.2f} pages/sec"
                
            # Calculate memory if available
            memory = "N/A"
            if "memory_usage" in stats and stats["memory_usage"]:
                latest_memory = stats["memory_usage"][-1]["memory_mb"]
                memory = f"{latest_memory:.2f} MB"
                
            worker_table.append([
                worker_id,
                stats.get("pages_crawled", 0),
                stats.get("urls_found", 0),
                stats.get("errors", 0),
                throughput,
                memory
            ])
        
        print(tabulate(
            worker_table,
            headers=["Worker ID", "Pages", "URLs Found", "Errors", "Throughput", "Memory"],
            tablefmt="pretty"
        ))
    
    print("==================================")

def generate_performance_graphs(num_workers, timestamps, pages_crawled, queue_sizes, active_workers, throughputs):
    """Generate performance graphs for the test"""
    # Create a directory for the graphs
    os.makedirs("performance_results", exist_ok=True)
    
    # Create DataFrame for easy plotting
    data = {
        'Time (s)': timestamps,
        'Pages Crawled': pages_crawled,
        'Queue Size': queue_sizes,
        'Active Workers': active_workers
    }
    
    # Add throughput data (it has one less point)
    if throughputs:
        if len(throughputs) < len(timestamps):
            throughputs = [0] + throughputs
        data['Throughput (pages/sec)'] = throughputs[:len(timestamps)]
    
    df = pd.DataFrame(data)
    
    # Create plots
    plt.figure(figsize=(12, 8))
    
    # Plot pages crawled over time
    plt.subplot(2, 2, 1)
    plt.plot(df['Time (s)'], df['Pages Crawled'])
    plt.title('Pages Crawled Over Time')
    plt.xlabel('Time (seconds)')
    plt.ylabel('Total Pages')
    plt.grid(True)
    
    # Plot throughput over time
    plt.subplot(2, 2, 2)
    plt.plot(df['Time (s)'], df['Throughput (pages/sec)'])
    plt.title('Throughput Over Time')
    plt.xlabel('Time (seconds)')
    plt.ylabel('Pages per Second')
    plt.grid(True)
    
    # Plot queue size over time
    plt.subplot(2, 2, 3)
    plt.plot(df['Time (s)'], df['Queue Size'])
    plt.title('Queue Size Over Time')
    plt.xlabel('Time (seconds)')
    plt.ylabel('URLs in Queue')
    plt.grid(True)
    
    # Plot active workers over time
    plt.subplot(2, 2, 4)
    plt.plot(df['Time (s)'], df['Active Workers'])
    plt.title('Active Workers Over Time')
    plt.xlabel('Time (seconds)')
    plt.ylabel('Count')
    plt.grid(True)
    
    plt.tight_layout()
    plt.savefig(f"performance_results/performance_{num_workers}_workers.png")
    
    # Also save raw data for further analysis
    df.to_csv(f"performance_results/performance_data_{num_workers}_workers.csv", index=False)
    
    print(f"Performance graphs saved to performance_results/performance_{num_workers}_workers.png")

async def run_scalability_tests(max_pages, seed_urls, worker_counts=[1, 2, 3], redis_host="localhost", redis_port=6379):
    """Run tests with different numbers of workers and compare results"""
    results = []
    
    for num_workers in worker_counts:
        # Run test with specified number of workers
        test_result = await run_performance_test(
            num_workers=num_workers,
            max_pages=max_pages,
            seed_urls=seed_urls,
            redis_host=redis_host,
            redis_port=redis_port
        )
        
        results.append(test_result)
        
        # Wait a bit between tests
        await asyncio.sleep(5)
    
    # Compare results
    print("\n" + "="*70)
    print("Scalability Test Results Comparison")
    print("="*70)
    
    comparison_table = []
    for result in results:
        comparison_table.append([
            result["num_workers"],
            result["pages_crawled"],
            f"{result['runtime']:.2f}s",
            f"{result['pages_per_second']:.2f}",
            f"{result['pages_per_worker']:.2f}",
            result["errors"]
        ])
    
    print(tabulate(
        comparison_table,
        headers=["Workers", "Pages Crawled", "Runtime", "Pages/sec", "Pages/worker", "Errors"],
        tablefmt="grid"
    ))
    
    # Generate comparison graph
    plt.figure(figsize=(10, 6))
    
    workers = [r["num_workers"] for r in results]
    throughput = [r["pages_per_second"] for r in results]
    
    plt.bar(workers, throughput)
    plt.title('Crawler Throughput vs. Number of Workers')
    plt.xlabel('Number of Workers')
    plt.ylabel('Pages per Second')
    plt.grid(True, axis='y')
    
    plt.savefig("performance_results/scalability_comparison.png")
    print("Scalability comparison saved to performance_results/scalability_comparison.png")
    
    # Save raw comparison data
    comparison_df = pd.DataFrame(results)
    comparison_df.to_csv("performance_results/scalability_comparison.csv", index=False)

async def main():
    parser = argparse.ArgumentParser(description='Distributed Crawler Performance Testing Script')
    parser.add_argument('--redis-host', default='localhost', help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port')
    parser.add_argument('--max-pages', type=int, default=1000, help='Maximum pages to crawl')
    parser.add_argument('--workers', type=int, nargs='+', default=[1, 2, 3], 
                        help='Number of workers to test (can specify multiple)')
    parser.add_argument('--seed-urls', nargs='+', required=True, 
                        help='Seed URLs to start crawling')
    parser.add_argument('--mode', choices=['single', 'scalability', 'fault-tolerance'], 
                        default='scalability', help='Test mode')
    
    args = parser.parse_args()
    
    if args.mode == 'single':
        # Run a single test with specified number of workers
        await run_performance_test(
            num_workers=args.workers[0],
            max_pages=args.max_pages,
            seed_urls=args.seed_urls,
            redis_host=args.redis_host,
            redis_port=args.redis_port
        )
    elif args.mode == 'scalability':
        # Run tests with different numbers of workers
        await run_scalability_tests(
            max_pages=args.max_pages,
            seed_urls=args.seed_urls,
            worker_counts=args.workers,
            redis_host=args.redis_host,
            redis_port=args.redis_port
        )
    elif args.mode == 'fault-tolerance':
        # Run a fault tolerance test (kills a worker mid-crawl)
        await run_performance_test(
            num_workers=max(2, args.workers[0]),  # Need at least 2 workers
            max_pages=args.max_pages,
            seed_urls=args.seed_urls,
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            fault_tolerance=True
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print("❌ Unhandled Exception:")
        traceback.print_exc()