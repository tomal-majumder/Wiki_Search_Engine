# 🔍 Distributed Web Search System

This project is a scalable and modular web search system composed of four major components:

- **Crawler** – Distributed web crawler built with Python and Redis
- **Indexer** – Spark-based inverted index builder with support for TF-IDF and BM25
- **Backend** – Node.js server exposing RESTful search APIs
- **Frontend** – React-based UI for querying and visualizing search results

Designed to run both locally and on High-Performance Computing Clusters (HPCC) using Slurm.

---

## 🚀 Live Demo

> 🧪 **Demo URL:** [http://18.191.52.209/](http://18.191.52.209/)

This is a demo deployment running on a limited test dataset (focused on football and world war content) due to compute and storage constraints.

⚠️ **Disclaimer**:

- The search results and ranking quality do not reflect full-scale system performance.
- Built and deployed using Docker on an EC2 t2.micro instance for demonstration purposes only.

---

### 🎥 Demo Preview

![Demo GIF](assets/demo.gif)

## Project Structure

```
.
├── Crawler/          # Async web crawler (Python + Redis)
├── Indexer/          # Inverted index builder (Spark scripts)
├── backend/          # REST API using Node.js and MongoDB
├── frontend/         # React UI for search and results
├── cluster/          # Slurm job scripts and HPCC config
├── scripts/ 		  # Data insertion scripts for MongoDB
└── README.md
```

---

## Local Development Setup

### Prerequisites

- Node.js (v18+), npm
- Python 3.9+, Redis
- Apache Spark (for local indexing)
- MongoDB
- Conda (recommended for Python environments)

---

### 1. Crawler (Local)

```bash
cd crawler
conda create -n crawler-env python=3.9
conda activate crawler-env
pip install -r requirements.txt
conda install -c conda-forge redis

# Start Redis server in another terminal
redis-server

# Run the crawler in worker mode with a seed URL
python crawler.py   --mode worker   --seed-urls https://en.wikipedia.org/wiki/Lionel_Messi   --allowed-domains en.wikipedia.org   --max-depth 2   --rate-limit 0.2   --concurrent 10   --max-page-limit 50
```

#### Crawler Arguments Explained

- `--mode`: Choose from `worker`, `manager`, `reset`, or `status`
- `--seed-urls`: List of URLs to start crawling
- `--allowed-domains`: Restrict crawling to these domains
- `--max-depth`: Depth of crawl tree (default: 3)
- `--rate-limit`: Seconds between requests (default: 0.5)
- `--concurrent`: Number of concurrent fetches (default: 10)
- `--max-page-limit`: Global maximum number of pages to crawl

To **reset** or **check status** of crawler:

```bash
python crawler.py --mode reset
python crawler.py --mode status
```

---

### 2. Indexer (Local)

```bash
cd indexer
pip install -r requirements.txt

# Run Spark job (ensure Spark is configured)
# Required: Spark 3.5.*, opendJDK-11
spark-submit inverted_index.py ../crawler/storage/   --output ../IndexeData/  --format csv   --use-stemming
```

#### Indexer Arguments Explained

- `input_path`: Path to folder with text files
- `--output`: Output folder path
- `--format`: Output format (`csv`, `json`, `text`, `parquet`)
- `--use-stemming`: Optional flag to enable stemming with NLTK

---

### 3. MongoDB Insert

To insert the crawled texts and images, run the following:

```bash
cd mongodb_scripts
pip install pymongo
python insertScript.py
```

To insert the inverted_index output, run:

```bash
python insertIndex.py
```

To insert the metadata (total number of documents and average doc len for BM25 scoring) output, run:

```bash
python metaDataInsert.py
```

---

### 3. Backend

Run the following commands to install necessary dependencies.

```bash
cd backend
# Install dependencies
pip install spacy nltk
# Download spaCy model
python -m spacy download en_core_web_sm
npm install
```

Create a .env file
In the `\backend` folder, create a file called `.env` and add your MongoDB connection string:

```bash
# For local MongoDB
MONGODB_URI=mongodb://127.0.0.1:27017/ir

# Or for MongoDB Atlas (Cloud)
# MONGODB_URI=mongodb+srv://<user>:<password>@cluster.mongodb.net/?retryWrites=true&w=majority

```

▶️ Start the server

```bash
npm start
```

The server will run at http://localhost:3001.

---

### 4. Frontend

```bash
cd frontend
npm install
npm start
```

You can now access the search engine web interface in http://localhost.com:3000.

---

## ☁️ Running on HPCC (Cluster Mode)

### Crawler

Run `cluster/distributed_crawler.sh`:

```bash
bash distributed_crawler.sh 3 15000
```

This will:

- Start Redis
- Reset state
- Queue seed URLs (configured inside the `distributed_crawler.sh`)
- Launch N worker jobs with Slurm

Here: number of workers is `3` and max page limit is `15000`.

### Indexer

Submit `cluster/spark_cluster.sh`:

```bash
sbatch spark_cluster.sh
```

Note: Ensure paths and environment variables are correct in the script.

---

---

## Technologies Used

| Layer      | Technologies                    |
| ---------- | ------------------------------- |
| Crawling   | Python, `aiohttp`, Redis, Slurm |
| Indexing   | Apache Spark, TF-IDF, BM25      |
| Backend    | Node.js, Express, MongoDB       |
| Frontend   | React, Axios, JavaScript        |
| Deployment | HPCC (Slurm), Local scripts     |

---

## Notes

- Crawled data is stored in `Crawler/storage` direcory and index files are stored in `IndexData/` (excluded from Git)
- Image files are named as `docID-count.jpg` in `storage/images`
