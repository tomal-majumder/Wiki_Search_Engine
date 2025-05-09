#!/bin/bash
#SBATCH --job-name=crawler-worker
#SBATCH --output=logs/crawler_%j.log
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=4:00:00

# This script expects the following environment variables to be set:
# REDIS_HOST, REDIS_PORT, MAX_PAGES, MAX_DEPTH, CONCURRENT, RATE_LIMIT, ALLOWED_DOMAINS

# Worker ID with unique timestamp
WORKER_ID="worker-$(date +%s)-${SLURM_JOB_ID}"

echo "Starting crawler worker $WORKER_ID"
echo "Connecting to Redis at $REDIS_HOST:$REDIS_PORT"
echo "Max pages: $MAX_PAGES"

# Set up Python environment
module load python/3.9 || echo "Python module not available, attempting to use conda"

# If Python module is not available, use conda
if [[ $(python3 -c "import sys; print(sys.version_info.major)" 2>/dev/null) != "3" ]]; then
    if ! conda env list | grep -q crawler_env; then
        conda create -y -n crawler_env python=3.9 redis aiohttp beautifulsoup4 pandas matplotlib tabulate psutil boto3
    fi
    source activate crawler_env
fi

# Install required packages if not already installed
pip install --user redis aiohttp beautifulsoup4 pandas matplotlib tabulate psutil boto3

# Run the crawler worker
python crawler.py \
  --mode worker \
  --redis-host $REDIS_HOST \
  --redis-port $REDIS_PORT \
  --worker-id "$WORKER_ID" \
  --max-depth $MAX_DEPTH \
  --concurrent $CONCURRENT \
  --rate-limit $RATE_LIMIT \
  --allowed-domains $ALLOWED_DOMAINS \
  --max-page-limit $MAX_PAGES \
  --log-file "logs/crawler_${SLURM_JOB_ID}.log" \
  --log-level INFO

# Save completion status
echo "Job completed at $(date)" >> logs/completion_${SLURM_JOB_ID}.txt