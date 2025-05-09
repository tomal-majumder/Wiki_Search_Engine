#!/usr/bin/env python3
import argparse
import redis
import json
import time
import os
import sys
from tabulate import tabulate
from datetime import datetime, timedelta

class CrawlerMonitor:
    def __init__(self, redis_host="localhost", redis_port=6379):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        
        # Redis keys
        self.queue_key = "crawler:queue"
        self.visited_key = "crawler:visited"
        self.active_workers_key = "crawler:active_workers"
        self.stats_key = "crawler:stats"
        self.global_counter_key = "crawler:global_page_count"
        
        # For rate tracking
        self.prev_count = 0
        self.prev_time = time.time()
        self.peak_rate = 0
        self.rate_history = []
        
        # For runtime average tracking
        self.start_time = time.time()
        self.start_count = self.get_queue_stats().get("global_count", 0)
        
    def get_queue_stats(self):
        """Get statistics about the job queue"""
        queue_size = self.redis_client.zcard(self.queue_key)
        visited_size = self.redis_client.scard(self.visited_key)
        global_count = int(self.redis_client.get(self.global_counter_key) or 0)
        
        return {
            "queue_size": queue_size,
            "visited_urls": visited_size,
            "global_count": global_count
        }
    
    def get_worker_stats(self):
        """Get statistics about active workers"""
        workers = {}
        
        # Get all worker stats
        all_stats = self.redis_client.hgetall(self.stats_key)
        for worker_id, stats_json in all_stats.items():
            try:
                workers[worker_id] = json.loads(stats_json)
            except json.JSONDecodeError:
                pass
                
        # Check which workers are active
        for worker_id in workers:
            heartbeat_key = f"crawler:worker_heartbeat:{worker_id}"
            workers[worker_id]["active"] = self.redis_client.exists(heartbeat_key) == 1
            
        return {
            "worker_count": len(workers),
            "active_workers": sum(1 for w in workers.values() if w.get("active", False)),
            "workers": workers
        }
    
    def calculate_rates(self, current_count):
        """Calculate current and peak crawl rates"""
        current_time = time.time()
        time_diff = current_time - self.prev_time
        
        if time_diff >= 1.0:  # At least 1 second has passed
            count_diff = current_count - self.prev_count
            current_rate = count_diff / time_diff
            
            # Update peak rate if necessary
            if current_rate > self.peak_rate:
                self.peak_rate = current_rate
            
            # Store rate in history (keep last 60 measurements)
            self.rate_history.append((current_time, current_rate))
            if len(self.rate_history) > 60:
                self.rate_history = self.rate_history[-60:]
            
            # Update previous values for next calculation
            self.prev_count = current_count
            self.prev_time = current_time
            
            return current_rate
        
        # Not enough time has passed
        return None
    
    def get_avg_rate(self, seconds=60):
        """Calculate average rate over the last X seconds"""
        if not self.rate_history:
            return 0
            
        # Filter to measurements within the time window
        cutoff_time = time.time() - seconds
        recent_rates = [(t, r) for t, r in self.rate_history if t >= cutoff_time]
        
        if not recent_rates:
            return 0
            
        # Calculate average
        return sum(r for _, r in recent_rates) / len(recent_rates)
    
    def get_runtime_average_throughput(self):
        """Calculate the average throughput throughout the entire runtime"""
        current_time = time.time()
        current_count = self.get_queue_stats().get("global_count", 0)
        
        runtime_seconds = current_time - self.start_time
        if runtime_seconds <= 0:
            return 0
        
        total_pages = current_count - self.start_count
        return total_pages / runtime_seconds
    
    def get_runtime_duration(self):
        """Get the formatted runtime duration"""
        runtime_seconds = time.time() - self.start_time
        return str(timedelta(seconds=int(runtime_seconds)))
    
    def display_status(self, clear_screen=True):
        """Display current crawler status with rates"""
        if clear_screen:
            os.system('cls' if os.name == 'nt' else 'clear')
            
        queue_stats = self.get_queue_stats()
        worker_stats = self.get_worker_stats()
        
        global_count = queue_stats["global_count"]
        current_rate = self.calculate_rates(global_count)
        
        # Only update if we calculated a new rate
        if current_rate is not None:
            current_rate_str = f"{current_rate:.2f} pages/sec"
        else:
            # Use the last calculated rate or 0
            last_rate = self.rate_history[-1][1] if self.rate_history else 0
            current_rate_str = f"{last_rate:.2f} pages/sec"
        
        # Get average rates for different time periods
        avg_rate_1min = self.get_avg_rate(60)
        avg_rate_5min = self.get_avg_rate(300)
        
        # Get runtime average throughput
        runtime_avg = self.get_runtime_average_throughput()
        runtime_duration = self.get_runtime_duration()
        
        # Current time
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"=== Distributed Crawler Status === ({current_time})")
        print(f"Total Pages Crawled: {global_count}")
        print(f"Queue Size: {queue_stats['queue_size']}")
        print(f"Visited URLs: {queue_stats['visited_urls']}")
        print(f"Active Workers: {worker_stats['active_workers']}/{worker_stats['worker_count']}")
        print(f"Monitor Runtime: {runtime_duration}")
        
        # Rate information
        print("\n=== Crawl Rate Information ===")
        print(f"Current Rate: {current_rate_str}")
        print(f"Peak Rate: {self.peak_rate:.2f} pages/sec")
        print(f"1-min Average: {avg_rate_1min:.2f} pages/sec")
        print(f"5-min Average: {avg_rate_5min:.2f} pages/sec")
        print(f"Runtime Average: {runtime_avg:.2f} pages/sec")
        
        # Worker table
        active_workers = []
        for worker_id, stats in worker_stats["workers"].items():
            if stats.get("active", False):
                worker_info = {
                    "ID": worker_id[-8:],  # Last 8 chars of ID
                    "Pages": stats.get("pages_crawled", 0),
                    "URLs Found": stats.get("urls_found", 0),
                    "Errors": stats.get("errors", 0)
                }
                
                # Calculate worker's throughput if available
                throughput = stats.get("throughput", [])
                if throughput:
                    recent = throughput[-1]
                    worker_info["Rate"] = f"{recent.get('pages_per_second', 0):.2f}/s"
                else:
                    worker_info["Rate"] = "N/A"
                    
                active_workers.append(worker_info)
        
        if active_workers:
            print("\n=== Active Workers ===")
            print(tabulate(active_workers, headers="keys", tablefmt="pretty"))
        
        print("=" * 40)

def main():
    parser = argparse.ArgumentParser(description='Distributed Crawler Monitor')
    parser.add_argument('--redis-host', default='localhost', help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port')
    parser.add_argument('--interval', type=float, default=5.0, help='Update interval in seconds')
    parser.add_argument('--no-clear', action='store_true', help='Do not clear screen between updates')
    
    args = parser.parse_args()
    
    monitor = CrawlerMonitor(redis_host=args.redis_host, redis_port=args.redis_port)
    
    print(f"Starting crawler monitor (Ctrl+C to exit)")
    print(f"Redis: {args.redis_host}:{args.redis_port}")
    print(f"Update interval: {args.interval} seconds")
    
    try:
        while True:
            monitor.display_status(clear_screen=not args.no_clear)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nMonitor stopped.")
    
if __name__ == "__main__":
    main()