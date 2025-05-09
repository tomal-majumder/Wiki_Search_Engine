#!/bin/bash

# distributed_crawler.sh - Main script for distributed crawler
# Usage: ./distributed_crawler.sh <num_workers> [max_pages]

# Check for required arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <num_workers> [max_pages]"
    echo "Example: $0 3 15000"
    exit 1
fi

# Parse command line arguments
NUM_WORKERS=$1
MAX_PAGES=${2:-15000}  # Default to 15000 if not specified

# Configuration
REDIS_PORT=6379
MAX_DEPTH=3
CONCURRENT=10
RATE_LIMIT=0.2
ALLOWED_DOMAINS="en.wikipedia.org"
SEED_URLS=(
    "https://en.wikipedia.org/wiki/Lionel_Messi"
    "https://en.wikipedia.org/wiki/World_War_II"
    "https://en.wikipedia.org/wiki/Rabindranath_Tagore"
)

echo "=== Starting Distributed Crawler Experiment ==="
echo "Number of workers: $NUM_WORKERS"
echo "Max pages: $MAX_PAGES"

# Step 1: Start Redis server
echo "1. Submitting Redis server job..."
REDIS_JOB_ID=$(sbatch redis_server.sh | awk '{print $4}')
echo "Redis server job submitted with ID: $REDIS_JOB_ID"

# Wait for Redis to start
echo "Waiting for Redis server to start..."
sleep 30

# Get the Redis host
REDIS_HOST=$(squeue --job $REDIS_JOB_ID --format=%N | tail -n 1)
if [ -z "$REDIS_HOST" ] || [ "$REDIS_HOST" == "NODELIST" ]; then
    echo "Failed to get Redis host. Check if the job started correctly."
    exit 1
fispark-submit \
        --master $master_url \
        --executor-memory 4G \
        --driver-memory 4G \
        --conf spark.executor.cores=4 \
        --conf spark.network.timeout=600s \
        --conf spark.executor.heartbeatInterval=60s \
        inverted_index.py $HOME/bigdata/Wiki_Search_Engine/crawler/storage/ \
        --output index_output --format csv --use-stemming

# Step 2: Reset crawler state
echo "2. Resetting crawler state..."
python crawler.py --mode reset --redis-host $REDIS_HOST --redis-port $REDIS_PORT
echo "Crawler state reset complete."

# Step 3: Add seed URLs
echo "3. Adding seed URLs to queue..."
# Construct seed URL parameter string
SEED_PARAM=""
for url in "${SEED_URLS[@]}"; do
    SEED_PARAM+="$url "
done

# Add seeds using manager mode
python crawler.py \
    --mode manager \
    --redis-host $REDIS_HOST \
    --redis-port $REDIS_PORT \
    --seed-urls $SEED_PARAM \
    --allowed-domains $ALLOWED_DOMAINS
echo "Seed URLs added to queue."

# Step 4: Launch worker instances
echo "4. Launching $NUM_WORKERS workers..."

for ((i=1; i<=$NUM_WORKERS; i++)); do
    echo "Submitting worker $i..."
    
    # Submit the job using the worker template
    sbatch \
        --export=REDIS_HOST=$REDIS_HOST,REDIS_PORT=$REDIS_PORT,MAX_PAGES=$MAX_PAGES,MAX_DEPTH=$MAX_DEPTH,CONCURRENT=$CONCURRENT,RATE_LIMIT=$RATE_LIMIT,ALLOWED_DOMAINS=$ALLOWED_DOMAINS \
        worker_template.sh
done

echo "All $NUM_WORKERS workers submitted."
echo "Monitor progress with: squeue -u $USER"

# Create the enhanced monitor script
echo "5. Setting up enhanced monitoring..."
# Copy the monitor script and make it executable
# Insert the crawler_monitor.py content here

# Set up the monitor launcher script
cat > monitor_status.sh << EOF
#!/bin/bash
# Monitor crawler status
./update_monitor_status.sh $REDIS_HOST 5
EOF

chmod +x monitor_status.sh

echo "Run './monitor_status.sh' in a separate terminal to monitor crawler progress with enhanced metrics."
echo "=== Distributed Crawler Experiment Setup Complete ==="