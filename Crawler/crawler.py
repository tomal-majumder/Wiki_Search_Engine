import asyncio
import aiohttp
import logging
import time
import hashlib
import redis
import json
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import boto3
import argparse
from botocore.exceptions import ClientError
from typing import List, Set, Dict, Any
import os
import signal
import sys
from dataclasses import dataclass, asdict
import uuid
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class CrawlJob:
    url: str
    depth: int = 0
    priority: int = 0
    parent_url: str = ""
    job_id: str = ""

    def __post_init__(self):
        if not self.job_id:
            self.job_id = str(uuid.uuid4())

class DistributedCrawler:
# Add to DistributedCrawler __init__ method:
    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        worker_id: str = None,
        max_depth: int = 3,
        concurrent_requests: int = 10,
        rate_limit: float = 0.5,
        respect_robots: bool = True,
        user_agent: str = "DistributedCrawler/1.0",
        allowed_domains: List[str] = None,
        s3_bucket: str = None,
        max_page_limit: int = None  # Add this parameter
    ):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.worker_id = worker_id or f"worker-{uuid.uuid4()}"
        self.max_depth = max_depth
        self.concurrent_requests = concurrent_requests
        self.rate_limit = rate_limit
        self.respect_robots = respect_robots
        self.user_agent = user_agent
        self.allowed_domains = set(allowed_domains) if allowed_domains else None
        self.s3_bucket = s3_bucket
        self.max_page_limit = max_page_limit  # Store the max page limit
        
        # Redis keys
        self.queue_key = "crawler:queue"
        self.visited_key = "crawler:visited"
        self.robots_key = "crawler:robots"
        self.active_workers_key = "crawler:active_workers"
        self.stats_key = "crawler:stats"
        self.global_counter_key = "crawler:global_page_count"  # Add global counter key
        self.unique_titles_key = "crawler:unique_titles_count"

        # Initialize worker statistics
        self.stats = {
            "pages_crawled": 0,
            "urls_found": 0,
            "start_time": time.time(),
            "errors": 0,
            "memory_usage": [],  # Track memory usage over time
            "unique_pages_stored": 0,  # Track unique pages stored
        }
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        
        logger.info(f"Initialized worker {self.worker_id}")
        
        # Start tracking memory usage
        self.start_memory_tracking()

    # Add memory tracking methods
    def start_memory_tracking(self):
        """Start periodic memory usage tracking"""
        def track_memory():
            import psutil
            process = psutil.Process(os.getpid())
            while True:
                try:
                    # Get memory info in MB
                    memory_info = process.memory_info()
                    memory_mb = memory_info.rss / (1024 * 1024)
                    
                    # Store with timestamp
                    memory_data = {
                        "timestamp": time.time(),
                        "memory_mb": memory_mb
                    }
                    
                    self.stats["memory_usage"].append(memory_data)
                    
                    # Keep only last 100 measurements to avoid excessive memory usage
                    if len(self.stats["memory_usage"]) > 100:
                        self.stats["memory_usage"] = self.stats["memory_usage"][-100:]
                        
                    time.sleep(5)  # Measure every 5 seconds
                except:
                    time.sleep(5)  # Continue even if there's an error
                    
        # Start memory tracking in a separate thread
        import threading
        memory_thread = threading.Thread(target=track_memory, daemon=True)
        memory_thread.start()

    # Modify the fetch method to check global page limit
    async def fetch(self, session: aiohttp.ClientSession, job: CrawlJob) -> Dict[str, Any]:
        """Fetch a URL and return the page content"""
        # First check if we've hit the global page limit
        if self.max_page_limit:
            current_count = int(self.redis_client.get(self.unique_titles_key) or 0)
            if current_count >= self.max_page_limit:
                logger.info(f"Unique page storage limit of {self.max_page_limit} reached. Stopping.")
                self.shutdown(None, None)
                return None

        
        start_time = time.time()
        try:
            url_hash = hashlib.md5(job.url.encode()).hexdigest()
            
            # Check if we've already processed this URL
            if self.redis_client.sismember(self.visited_key, url_hash):
                logger.debug(f"Skipping already visited URL: {job.url}")
                return None
                
            # Mark URL as visited to prevent duplicate processing
            self.redis_client.sadd(self.visited_key, url_hash)
            
            # Check robots.txt restrictions if enabled
            if self.respect_robots and not self.is_allowed_by_robots(job.url):
                logger.debug(f"URL blocked by robots.txt: {job.url}")
                return None
                
            # Rate limiting
            await asyncio.sleep(self.rate_limit)
            
            logger.info(f"Fetching URL: {job.url}")
            async with session.get(job.url, headers={"User-Agent": self.user_agent}) as response:
                if response.status != 200:
                    logger.warning(f"Failed to fetch {job.url}: HTTP {response.status}")
                    return None
                    
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' not in content_type.lower():
                    logger.debug(f"Skipping non-HTML content at {job.url}: {content_type}")
                    return None
                    
                html = await response.text()
                
                result = {
                    "url": job.url,
                    "status": response.status,
                    "content_type": content_type,
                    "html": html,
                    "depth": job.depth,
                    "parent_url": job.parent_url,
                    "timestamp": time.time(),
                    "worker_id": self.worker_id,
                    "fetch_time": time.time() - start_time
                }
                
                # Update both local and global page counters
                self.stats["pages_crawled"] += 1
                
                # Increment global counter atomically
                #self.redis_client.incr(self.global_counter_key)
                
                # Check if we've hit the global page limit after incrementing
                if self.max_page_limit:
                    current_count = int(self.redis_client.get(self.global_counter_key) or 0)
                    if current_count >= self.max_page_limit:
                        logger.info(f"Global page limit of {self.max_page_limit} reached. Stopping.")
                        self.shutdown(None, None)
                
                return result
                
        except Exception as e:
            logger.error(f"Error fetching {job.url}: {str(e)}")
            self.stats["errors"] += 1
            return None
    # Add a method to check robots.txt
    # This is a simplified implementation
    # A real implementation would fetch and parse robots.txt
    # and respect its rules

    def is_allowed_by_robots(self, url: str) -> bool:
        """Check if URL is allowed by robots.txt"""
        # This is a simplified implementation
        # A real implementation would parse robots.txt and respect its rules
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        # Check if we have cached robots.txt info
        robots_cache_key = f"{self.robots_key}:{domain}"
        if self.redis_client.exists(robots_cache_key):
            return self.redis_client.sismember(robots_cache_key, "allowed")
            
        # For now, we'll just allow everything
        # In a real implementation, you'd fetch and parse robots.txt
        self.redis_client.sadd(robots_cache_key, "allowed")
        self.redis_client.expire(robots_cache_key, 3600)  # Cache for 1 hour
        return True

    async def parse_links(self, job: CrawlJob, html: str) -> List[str]:
        """Extract links from HTML content"""
        if job.depth > self.max_depth:
            return []
            
        try:
            soup = BeautifulSoup(html, 'html.parser')
            base_url = job.url
            links = []
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                if not href or href.startswith('#'):
                    continue
                    
                # Normalize the URL
                full_url = urljoin(base_url, href)
                parsed = urlparse(full_url)
                
                # Skip non-HTTP(S) URLs
                if parsed.scheme not in ('http', 'https'):
                    continue
                    
                # Skip non-content pages
                if any(pattern in full_url for pattern in [
                    'action=edit', 
                    'action=history', 
                    'Special:', 
                    'File:', 
                    'Talk:', 
                    'User:', 
                    'index.php?'
                ]):
                    continue
                    
                # Check if the domain is allowed
                if self.allowed_domains and parsed.netloc not in self.allowed_domains:
                    continue
                    
                links.append(full_url)
                
            self.stats["urls_found"] += len(links)
            return links
            
        except Exception as e:
            logger.error(f"Error parsing links from {job.url}: {str(e)}")
            return []
    async def process_page(self, session: aiohttp.ClientSession, job: CrawlJob):
        """Process a single web page"""
        result = await self.fetch(session, job)
        if not result:
            return
            
        # Store the result
        await self.store_result(result)
        
        # Extract and queue new links if we haven't reached max depth
        if job.depth < self.max_depth:
            links = await self.parse_links(job, result["html"])
            for link in links:
                new_job = CrawlJob(
                    url=link,
                    depth=job.depth + 1,
                    parent_url=job.url
                )
                await self.queue_job(new_job)

    
    async def store_result(self, result: Dict[str, Any]):
        """Store the crawling result with extracted text content only"""
        try:
            # Extract meaningful content from HTML using BeautifulSoup
            soup = BeautifulSoup(result["html"], 'html.parser')

             # Create storage directories
            os.makedirs("storage", exist_ok=True)
            os.makedirs("storage/images", exist_ok=True)
            # os.makedirs("storage/debug", exist_ok=True)  

            # Extract page title
            title = soup.title.string if soup.title else "No title"
            title = title.strip()
            # Normalize title for deduplication
            # Remove trailing "- Wikipedia" and normalize
            normalized_title = re.sub(r'\s*-\s*wikipedia$', '', title, flags=re.IGNORECASE).strip().lower()
            title_hash = hashlib.md5(normalized_title.encode()).hexdigest()
            title_key = f"crawler:title_hash:{title_hash}"

            # Atomically claim title across all workers
            claimed = self.redis_client.set(title_key, "1", nx=True)
            if not claimed:
                logger.info(f"Duplicate title detected: '{title}' — skipping storage")
                return  # Stop here to avoid duplicate saving
            # Title was unique → increment the unique pages counter
            self.redis_client.incr(self.unique_titles_key)
            self.stats["unique_pages_stored"] += 1

            url_hash = hashlib.md5(result["url"].encode()).hexdigest()
            img_count = 0
            MAX_IMAGES = 10
            
            # Find all images with src containing .jpg or .jpeg
            images = soup.find_all('img', {'src': re.compile('.jpg|.jpeg', re.IGNORECASE)})
            
            for img in images:
                if img_count >= MAX_IMAGES:
                    break
                    
                img_src = img['src']
                if not img_src:
                    continue
                
                # Handle protocol-relative URLs (starting with //)
                if img_src.startswith('//'):
                    # Use https as the default protocol
                    img_src = f"https:{img_src}"
                    
                # Normalize the URL
                img_url = urljoin(result["url"], img_src)
                # logger.info(f"Saving.. image content for '{img_url}'")
                try:
                    # Download the image
                    async with aiohttp.ClientSession() as session:
                        async with session.get(img_url, timeout=10) as response:
                            if response.status == 200:
                                # Save image with docID-count.jpg format
                                img_path = f"storage/images/{url_hash}-{img_count}.jpg"
                                with open(img_path, 'wb') as f:
                                    f.write(await response.read())
                                    
                                img_count += 1
                                logger.debug(f"Saved image {img_count}: {img_url} -> {img_path}")
                except Exception as e:
                    logger.error(f"Error downloading image {img_url}: {str(e)}")
                    continue
            # Remove unwanted elements from the soup
            for element in soup.select('.mw-editsection, .navbox, #mw-navigation, #footer, .sidebar, .infobox, script, style, .reference, .references'):
                if element:
                    element.decompose()

            # For Wikipedia, extract the main content
            main_content = soup.select_one('#mw-content-text')
            
            # Extract just the text content
            if main_content:
                # Get all paragraphs and headings
                content_elements = main_content.select('p, h1, h2, h3, h4, h5, h6')
                
                # Build a text representation with some structure
                content_text = []
                for element in content_elements:
                    # For headings, add formatting
                    if element.name.startswith('h'):
                        heading_level = int(element.name[1])
                        prefix = '#' * heading_level + ' '
                        content_text.append(f"\n{prefix}{element.get_text().strip()}\n")
                    # For paragraphs, just add the text
                    elif element.name == 'p' and element.get_text().strip():
                        content_text.append(element.get_text().strip())
                
                extracted_content = "\n\n".join(content_text)
            else:
                # Fallback to getting all text if main content not found
                extracted_content = soup.get_text(" ", strip=True)

            result_to_store = result.copy()
            result_to_store.pop("html", "")
            result_to_store["title"] = title
            result_to_store["image_count"] = img_count
        
            # Save metadata to Redis
            self.redis_client.hset(f"crawler:results:{url_hash}", mapping=result_to_store)
            # Store content to local storage with better filename
            #os.makedirs("storage", exist_ok=True)
            filename = f"storage/{url_hash}.txt"  # Change to .txt for plain text
            
            # Save the content with title and URL
            with open(filename, "w", encoding="utf-8") as f:
                f.write(f"Title: {title}\n")
                f.write(extracted_content)
                
            logger.info(f"Saved text content for '{title}' locally: {filename}")
        
        except Exception as e:
            logger.error(f"Error storing result for {result['url']}: {str(e)}")
            # Fallback to saving original HTML
            url_hash = hashlib.md5(result["url"].encode()).hexdigest()
            with open(f"storage/{url_hash}.html", "w", encoding="utf-8") as f:
                f.write(result["html"])
    
    async def queue_job(self, job: CrawlJob):
        """Add a job to the distributed queue"""
        job_data = json.dumps(asdict(job))
        try:
            # Add to the sorted set with priority
            self.redis_client.zadd(self.queue_key, {job_data: job.priority})
            logger.debug(f"Queued job for {job.url} at depth {job.depth}")
        except Exception as e:
            logger.error(f"Failed to queue job: {str(e)}")

    async def get_next_job(self) -> CrawlJob:
        """Get the next job from the queue"""
        try:
            # Get the highest priority (lowest score) job
            result = self.redis_client.zpopmin(self.queue_key, 1)
            if not result:
                return None
                
            job_data, _ = result[0]
            return CrawlJob(**json.loads(job_data))
        except Exception as e:
            logger.error(f"Failed to get next job: {str(e)}")
            return None

    async def register_worker(self):
        """Register this worker with the cluster"""
        worker_data = {
            "id": self.worker_id,
            "start_time": time.time(),
            "hostname": os.uname()[1]
        }
        self.redis_client.hset(self.active_workers_key, self.worker_id, json.dumps(worker_data))
        # Set a key that expires if this worker dies
        self.redis_client.setex(f"crawler:worker_heartbeat:{self.worker_id}", 60, "alive")


    # Add throughput calculation to heartbeat method
    async def heartbeat(self):
        """Send regular heartbeats to indicate the worker is alive"""
        last_pages = 0
        last_time = time.time()
        
        while True:
            try:
                current_time = time.time()
                current_pages = self.stats["pages_crawled"]
                
                # Calculate throughput (pages per second)
                time_diff = current_time - last_time
                if time_diff > 0:
                    pages_diff = current_pages - last_pages
                    throughput = pages_diff / time_diff
                    
                    # Add throughput to stats
                    if "throughput" not in self.stats:
                        self.stats["throughput"] = []
                    
                    self.stats["throughput"].append({
                        "timestamp": current_time,
                        "pages_per_second": throughput
                    })
                    
                    # Keep only last 100 measurements
                    if len(self.stats["throughput"]) > 100:
                        self.stats["throughput"] = self.stats["throughput"][-100:]
                        
                    # Update last values
                    last_pages = current_pages
                    last_time = current_time
                
                # Send heartbeat and stats
                self.redis_client.setex(f"crawler:worker_heartbeat:{self.worker_id}", 60, "alive")
                self.redis_client.hset(self.stats_key, self.worker_id, json.dumps(self.stats))
                await asyncio.sleep(10)  # Shorter interval for more frequent metrics
            except Exception as e:
                logger.error(f"Heartbeat error: {str(e)}")
                await asyncio.sleep(5)

    async def run_worker(self):
        """Main worker loop to process jobs from the queue"""
        await self.register_worker()
        
        # Start the heartbeat task
        heartbeat_task = asyncio.create_task(self.heartbeat())
        
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    job = await self.get_next_job()
                    if not job:
                        # No jobs available, wait before trying again
                        logger.debug("No jobs available, waiting...")
                        await asyncio.sleep(1)
                        continue
                        
                    await self.process_page(session, job)
                    
                except Exception as e:
                    logger.error(f"Worker error: {str(e)}")
                    await asyncio.sleep(1)

    async def crawl(self, seed_urls: List[str]):
        """Start crawling from seed URLs"""
        # Add seed URLs to the queue
        for url in seed_urls:
            job = CrawlJob(url=url, depth=0, priority=0)
            await self.queue_job(job)
            
        # Start the worker
        await self.run_worker()

    def shutdown(self, signum, frame):
        """Handle graceful shutdown"""
        logger.info(f"Worker {self.worker_id} shutting down...")
        # Remove worker from active workers
        self.redis_client.hdel(self.active_workers_key, self.worker_id)
        self.redis_client.delete(f"crawler:worker_heartbeat:{self.worker_id}")
        
        # Update final stats
        self.stats["end_time"] = time.time()
        self.stats["runtime"] = self.stats["end_time"] - self.stats["start_time"]
        self.redis_client.hset(self.stats_key, self.worker_id, json.dumps(self.stats))
        
        sys.exit(0)

class CrawlerManager:
    """Class to manage and monitor the distributed crawler"""
    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        s3_bucket: str = None
    ):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.s3_bucket = s3_bucket
        
        # Redis keys - same as in crawler
        self.queue_key = "crawler:queue"
        self.visited_key = "crawler:visited"
        self.active_workers_key = "crawler:active_workers"
        self.stats_key = "crawler:stats"
        self.title_dedup_prefix = "crawler:title_hash:"

    def clear_queue(self):
        """Clear the job queue"""
        self.redis_client.delete(self.queue_key)
        logger.info("Job queue cleared")

    # Modify the CrawlerManager class to support resetting the page counter
    def reset_crawler(self):
        """Reset the crawler state (for a fresh start)"""
        keys_to_delete = [
            self.queue_key,
            self.visited_key,
            self.stats_key,
            "crawler:unique_titles_count"  # Also reset the global page counter
        ]
        
        for key in keys_to_delete:
            self.redis_client.delete(key)
        # Clear title hash dedup keys
        title_pattern = "crawler:title_hash:*"
        for key in self.redis_client.scan_iter(title_pattern):
            self.redis_client.delete(key)
        logger.info("Cleared title-based deduplication keys")
    
        # Delete all worker heartbeats
        for key in self.redis_client.scan_iter("crawler:worker_heartbeat:*"):
            self.redis_client.delete(key)
            
        # Clear active workers
        self.redis_client.delete(self.active_workers_key)
        
        logger.info("Crawler state reset")

    def get_queue_stats(self) -> Dict[str, Any]:
        """Get statistics about the job queue"""
        queue_size = self.redis_client.zcard(self.queue_key)
        visited_size = self.redis_client.scard(self.visited_key)
        
        return {
            "queue_size": queue_size,
            "visited_urls": visited_size
        }

    def get_worker_stats(self) -> Dict[str, Any]:
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

    def get_aggregate_stats(self) -> Dict[str, Any]:
        """Get aggregate statistics across all workers"""
        workers = self.get_worker_stats()["workers"]
        
        pages_crawled = sum(w.get("pages_crawled", 0) for w in workers.values())
        urls_found = sum(w.get("urls_found", 0) for w in workers.values())
        errors = sum(w.get("errors", 0) for w in workers.values())
        
        queue_stats = self.get_queue_stats()
        
        return {
            "total_pages_crawled": pages_crawled,
            "total_urls_found": urls_found,
            "total_errors": errors,
            "queue_size": queue_stats["queue_size"],
            "visited_urls": queue_stats["visited_urls"],
            "active_workers": workers
        }

    def print_status(self):
        """Print the current status of the distributed crawler"""
        queue_stats = self.get_queue_stats()
        worker_stats = self.get_worker_stats()
        agg_stats = self.get_aggregate_stats()
        
        print("=== Distributed Crawler Status ===")
        print(f"Queue Size: {queue_stats['queue_size']}")
        print(f"Visited URLs: {queue_stats['visited_urls']}")
        print(f"Active Workers: {worker_stats['active_workers']}/{worker_stats['worker_count']}")
        print(f"Pages Crawled: {agg_stats['total_pages_crawled']}")
        print(f"URLs Found: {agg_stats['total_urls_found']}")
        print(f"Errors: {agg_stats['total_errors']}")
        print("=================================")

async def main():
    parser = argparse.ArgumentParser(description='Distributed Web Crawler')
    parser.add_argument('--redis-host', default='localhost', help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port')
    parser.add_argument('--s3-bucket', help='S3 bucket for storing crawled content')
    parser.add_argument('--max-depth', type=int, default=3, help='Maximum crawl depth')
    parser.add_argument('--concurrent', type=int, default=10, help='Concurrent requests')
    parser.add_argument('--rate-limit', type=float, default=0.5, help='Seconds between requests')
    parser.add_argument('--user-agent', default='DistributedCrawler/1.0', help='User agent string')
    parser.add_argument('--worker-id', help='Unique worker ID (generated if not provided)')
    parser.add_argument('--seed-urls', nargs='+', help='Seed URLs to start crawling')
    parser.add_argument('--allowed-domains', nargs='+', help='Allowed domains to crawl')
    parser.add_argument('--mode', choices=['worker', 'manager', 'reset', 'status'], 
                        default='worker', help='Operation mode')
    parser.add_argument('--max-page-limit', type=int, help='Maximum number of pages to crawl globally')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--log-file', default='crawler.log', help='Log file path')
    parser.add_argument('--log-level', default='INFO', help='Log level (DEBUG, INFO, WARNING, ERROR)')

    args = parser.parse_args()
    
    if args.mode == 'worker':
        if not args.seed_urls and not args.redis_host:
            parser.error("Worker mode requires either seed URLs or a Redis host with existing jobs")
            
        crawler = DistributedCrawler(
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            worker_id=args.worker_id,
            max_depth=args.max_depth,
            concurrent_requests=args.concurrent,
            rate_limit=args.rate_limit,
            user_agent=args.user_agent,
            allowed_domains=args.allowed_domains,
            s3_bucket=args.s3_bucket,
            max_page_limit=args.max_page_limit
        )
        
        if args.seed_urls:
            await crawler.crawl(args.seed_urls)
        else:
            await crawler.run_worker()
            
    elif args.mode == 'manager':
        manager = CrawlerManager(
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            s3_bucket=args.s3_bucket
        )
        
        if args.seed_urls:
            # Add seed URLs to the queue
            crawler = DistributedCrawler(
                redis_host=args.redis_host,
                redis_port=args.redis_port
            )
            
            for url in args.seed_urls:
                job = CrawlJob(url=url, depth=0, priority=0)
                await crawler.queue_job(job)
                
            print(f"Added {len(args.seed_urls)} seed URLs to the queue")
            
        # Print initial status
        manager.print_status()
        
    elif args.mode == 'reset':
        manager = CrawlerManager(
            redis_host=args.redis_host,
            redis_port=args.redis_port
        )
        manager.reset_crawler()
        print("Crawler state has been reset")
        
    elif args.mode == 'status':
        manager = CrawlerManager(
            redis_host=args.redis_host,
            redis_port=args.redis_port
        )
        manager.print_status()

if __name__ == "__main__":
    asyncio.run(main())
    