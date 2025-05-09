#!/bin/bash
#SBATCH --job-name=spark_cluster
#SBATCH --output=spark_%j.log
#SBATCH --nodes=3                # Adjust this as needed
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=3:00:00

# Set Spark environment
export SPARK_HOME=$HOME/spark-3.5.5-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH

# Create log directory
mkdir -p $HOME/spark_logs

# Get node list and master node
nodes=($(scontrol show hostnames $SLURM_NODELIST))
master_node=${nodes[0]}
master_ip=$(srun --nodes=1 --ntasks=1 --nodelist=$master_node hostname -I | awk '{print $1}')
export SPARK_MASTER_HOST=$master_ip
export SPARK_LOCAL_IP=$master_ip

echo "Master node: $master_node"
echo "Master IP: $master_ip"
echo "All nodes: ${nodes[@]}"

# Function to check the number of connected workers
check_workers() {
    echo "Checking connected workers..."
    local attempts=0
    local max_attempts=10
    local expected_workers=${#nodes[@]}  # Total number of expected workers
    local connected_workers=0
    
    while [ $attempts -lt $max_attempts ] && [ $connected_workers -lt $expected_workers ]; do
        # Try to get worker count from Spark master UI using curl
        if connected_workers=$(curl -s http://$SPARK_MASTER_HOST:8080/json/ 2>/dev/null | 
                              python3 -c "import sys, json; print(len(json.load(sys.stdin).get('workers', [])))" 2>/dev/null); then
            if [ -z "$connected_workers" ]; then
                connected_workers=0
            fi
            
            echo "Connected workers: $connected_workers of $expected_workers"
            
            if [ "$connected_workers" -eq "$expected_workers" ]; then
                echo "All workers connected successfully!"
                return 0
            fi
        else
            echo "Failed to fetch worker information. Spark master UI may not be available yet."
            connected_workers=0
        fi
        
        attempts=$((attempts + 1))
        if [ $attempts -lt $max_attempts ]; then
            echo "Waiting for more workers to connect (attempt $attempts/$max_attempts)..."
            sleep 10
        fi
    done
    
    if [ $connected_workers -lt $expected_workers ]; then
        echo "Warning: Not all workers connected. Expected $expected_workers, got $connected_workers."
        echo "Continuing anyway, but performance may be affected."
        return 1
    fi
    
    return 0
}

# Start Spark master on the master node
if [[ $(hostname -s) == ${master_node%%.*} || $(hostname) == ${master_node} ]]; then
    echo "Starting Spark master on $master_node"
    $SPARK_HOME/sbin/stop-all.sh > /dev/null 2>&1
    SPARK_LOCAL_IP=$master_ip $SPARK_HOME/sbin/start-master.sh --host $master_ip
    sleep 10

    master_url="spark://$master_ip:7077"
    echo "Master URL: $master_url"

    # Start worker on master node
    echo "Starting worker on master node"
    SPARK_LOCAL_IP=$master_ip $SPARK_HOME/sbin/start-worker.sh $master_url

    # Launch workers on the remaining nodes using srun
    for worker_node in "${nodes[@]:1}"; do
        echo "Starting worker on $worker_node"
        srun --nodes=1 --ntasks=1 --nodelist=$worker_node --exclusive bash -c "
            export SPARK_HOME=$SPARK_HOME
            export PATH=\$SPARK_HOME/bin:\$PATH
            export SPARK_LOCAL_IP=\$(hostname -I | awk '{print \$1}')
            \$SPARK_HOME/sbin/start-worker.sh $master_url
            sleep infinity
        " &
    done

    echo "Waiting for workers to connect..."
    sleep 30
    
    # Check if all workers are connected
    check_workers
    
    # Submit your Spark job
    echo "Submitting Spark job..."
    spark-submit \
        --master $master_url \
        --executor-memory 4G \
        --driver-memory 4G \
        --conf spark.executor.cores=4 \
        --conf spark.network.timeout=600s \
        --conf spark.executor.heartbeatInterval=60s \
        inverted_index.py $HOME/bigdata/Wiki_Search_Engine/crawler/storage/ \
        --output index_output --format csv --use-stemming


    echo "Stopping Spark cluster..."
    $SPARK_HOME/sbin/stop-all.sh
    echo "Done."
else
    echo "This is not the master node. Exiting."
fis