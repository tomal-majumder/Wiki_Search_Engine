#!/bin/bash
#SBATCH --job-name=redis-server
#SBATCH --output=logs/redis_server_%j.log
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=8:00:00

echo "Starting Redis server on $(hostname) at $(date)"

# Create logs directory if it doesn't exist
mkdir -p logs

# Check if Redis is available as a module
if command -v module &> /dev/null; then
    module load redis || echo "Redis module not available, attempting to use conda"
fi

# If Redis is not available as a module, use conda
if ! command -v redis-server &> /dev/null; then
    if ! conda env list | grep -q crawler_env; then
        conda create -y -n crawler_env python=3.9 redis
    fi
    source activate crawler_env
    pip install --user redis
fi

# Find Redis server executable
REDIS_SERVER=$(which redis-server || echo "redis-server not found")

if [ "$REDIS_SERVER" == "redis-server not found" ]; then
    echo "Redis server not found. Installing Redis..."
    
    # Create temp directory for Redis installation
    TEMP_DIR=$(mktemp -d)
    cd $TEMP_DIR
    
    # Download and install Redis
    wget http://download.redis.io/redis-stable.tar.gz
    tar xvzf redis-stable.tar.gz
    cd redis-stable
    make
    
    REDIS_SERVER="$TEMP_DIR/redis-stable/src/redis-server"
fi

# Create Redis configuration
REDIS_CONF="redis.conf"
cat > $REDIS_CONF << EOF
bind 0.0.0.0
port 6379
protected-mode no
daemonize no
logfile "logs/redis_${SLURM_JOB_ID}.log"
EOF

echo "Starting Redis server..."
echo "Redis will be accessible at $(hostname):6379"

# Start Redis server
$REDIS_SERVER $REDIS_CONF

# This script doesn't return until Redis is terminated