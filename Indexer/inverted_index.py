#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, explode, col, lit, regexp_replace, udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import Tokenizer, StopWordsRemover
import re
import os
import sys
import argparse

# Check if NLTK is available for stemming
try:
    import nltk
    from nltk.stem import PorterStemmer
    nltk_available = True
except ImportError:
    nltk_available = False

def clean_text(text):
    if text is None:
        return ""
    # Fixed regex to properly handle whitespace
    return re.sub(r'[^a-zA-Z0-9\s]', '', text.lower())

# def apply_stemming(tokens):
#     """Apply Porter stemming to a list of tokens."""
#     if not tokens:
#         return []
    
#     stemmer = PorterStemmer()
#     return [stemmer.stem(token) for token in tokens if token]
def apply_stemming(tokens):
    """Apply Porter stemming to a list of tokens."""
    if not tokens:
        return []
    
    try:
        # Import inside function to ensure it's available
        from nltk.stem import PorterStemmer
        stemmer = PorterStemmer()
        return [stemmer.stem(token) for token in tokens if token]
    except (ImportError, NameError):
        # If stemming fails, return original tokens
        print("Warning: Porter stemmer not available, returning original tokens")
        return [token for token in tokens if token]
def ensure_nltk():
    global nltk_available
    try:
        import nltk
        from nltk.stem import PorterStemmer
        try:
            # Test if punkt is already downloaded
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            nltk.download('punkt', quiet=True)
        nltk_available = True
        return True
    except ImportError:
        nltk_available = False
        return False
# Add this function after your main() function in inverted_index.py
def print_cluster_info(spark):
    """Print diagnostic information about the Spark cluster."""
    print("\n===== SPARK CLUSTER INFORMATION =====")
    try:
        sc = spark.sparkContext
        
        # Get cluster information
        app_id = sc.applicationId
        ui_url = sc.uiWebUrl
        
        print(f"Application ID: {app_id}")
        print(f"Web UI: {ui_url}")
        
        # Get executor information
        executor_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1  # -1 to exclude driver
        print(f"Number of executors: {executor_count}")
        
        # Print executor details
        executors = sc._jsc.sc().getExecutorMemoryStatus().keySet().toArray()
        print("Executor hosts:")
        for executor in executors:
            print(f"  - {executor}")
        
        # Get Spark configuration
        print("\nSpark Configuration:")
        print(f"  Master: {sc.master}")
        print(f"  Deployment mode: {sc.deployMode}")
        print(f"  Default parallelism: {sc.defaultParallelism}")
        
        # Test parallel execution
        print("\nTesting parallel execution...")
        nodes = sc.parallelize(range(100), 10).mapPartitions(lambda _: [sc._jvm.java.net.InetAddress.getLocalHost().getHostName()]).distinct().collect()
        print(f"Nodes executing tasks: {nodes}")
        print(f"Number of unique nodes: {len(nodes)}")
        
    except Exception as e:
        print(f"Error getting cluster info: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("====================================\n")
    
def main():
    global nltk_available

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Create an inverted index using Spark')
    parser.add_argument('input_path', help='Path to the text files directory')
    parser.add_argument('--output', default='index_output', help='Output directory for the index')
    parser.add_argument('--format', default='parquet', choices=['parquet', 'csv', 'json', 'text'], 
                        help='Output format (parquet, csv, json, or text)')
    parser.add_argument('--use-stemming', action='store_true', help='Apply Porter stemming to tokens')
    args = parser.parse_args()
    
    input_path = args.input_path
    output_path = os.path.join(os.getcwd(), args.output)
    use_stemming = args.use_stemming
    output_format = args.format
    
    print(f"Starting inverted index creation on {input_path}")
    print(f"Output will be saved to {output_path} in {output_format} format")
    print(f"Stemming enabled: {use_stemming}")
    

    
    # If stemming is requested but still not available, disable it
    if use_stemming and not nltk_available:
        print("Stemming will be disabled due to missing dependencies.")
        use_stemming = False
    
    # Create Spark session with more memory and tuned configuration
    spark = SparkSession.builder \
        .appName("InvertedIndex") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", 12) \
        .config("spark.default.parallelism", 12) \
        .getOrCreate()
    
    # Check if input path exists
    if not os.path.exists(input_path):
        print(f"Error: Input path {input_path} does not exist")
        spark.stop()
        return
    
    # Get file list to verify we have data
    files = os.listdir(input_path)
    print(f"Found {len(files)} files in input directory")
    
    try:
        print("Reading text files...")
        # Read all text files with explicit schema to avoid inference issues
        df = spark.read.text(input_path).withColumn("filename", input_file_name())
        
        # Print schema and sample data for debugging
        print("Schema after reading files:")
        df.printSchema()
        print("Sample data:")
        df.show(5, truncate=False)
        
        # Apply text cleaning function
        print("Cleaning text...")
        clean_udf = spark.udf.register("clean_text", clean_text)
        df = df.withColumn("cleaned", clean_udf(df['value']))
        
        # Tokenize and remove stop words
        print("Tokenizing and removing stop words...")
        tokenizer = Tokenizer(inputCol="cleaned", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        df = tokenizer.transform(df)
        df = remover.transform(df)
        
        if use_stemming:
            print("Applying Porter stemming...")
            try:
                # Ensure NLTK is available before attempting stemming
                if not nltk_available and not ensure_nltk():
                    print("NLTK still not available, skipping stemming")
                    token_col = "filtered"
                else:
                    stemming_udf = udf(apply_stemming, ArrayType(StringType()))
                    df = df.withColumn("stemmed", stemming_udf(col("filtered")))
                    token_col = "stemmed"
            except Exception as e:
                print(f"Error applying stemming: {str(e)}")
                print("Falling back to non-stemmed tokens")
                token_col = "filtered"
        
        # Extract filename from full path to make output more readable
        df = df.withColumn("filename", regexp_replace(df["filename"], "^.*/", ""))
        
        # Count documents for calculating IDF
        print("Counting documents...")
        total_docs = df.select("filename").distinct().count()
        print(f"Total unique documents: {total_docs}")
        
        # Explode tokens and calculate term frequencies
        print(f"Calculating term frequencies using {token_col} column...")
        words_df = df.select("filename", explode(col(token_col)).alias("term"))
        words_df = words_df.filter(col("term").isNotNull() & (col("term") != ""))
        
        print("Calculating document frequencies...")
        tf_df = words_df.groupBy("term", "filename").count().withColumnRenamed("count", "tf")
        df_doc_len = words_df.groupBy("filename").count().withColumnRenamed("count", "doc_len")
        df_df = words_df.distinct().groupBy("term").count().withColumnRenamed("count", "df")
        
        # Join dataframes to calculate TF-IDF
        print("Joining dataframes and calculating TF-IDF...")
        joined_df = tf_df.join(df_doc_len, "filename").join(df_df, "term")
        indexed_df = joined_df.withColumn("tfidf", 
                                        col("tf") * (lit(total_docs)/col("df"))) \
                            .select("term", "filename", "tf", "df", "doc_len", "tfidf")
        
        # Save output in the specified format
        print(f"Writing results to {output_path} in {output_format} format...")
        
        if output_format == 'parquet':
            indexed_df.write.mode("overwrite").parquet(output_path)
        elif output_format == 'csv':
            indexed_df.write.mode("overwrite").option("header", "true").csv(output_path)
        elif output_format == 'json':
            indexed_df.write.mode("overwrite").json(output_path)
        elif output_format == 'text':
            # For text format, save as a single text file for easier reading
            indexed_df.coalesce(1).write.mode("overwrite").format("csv") \
                      .option("header", "true").option("delimiter", "\t") \
                      .save(f"{output_path}_tmp")
            
            # Rename the part file to a more readable name
            import glob
            part_file = glob.glob(f"{output_path}_tmp/part-*")[0]
            os.makedirs(output_path, exist_ok=True)
            os.rename(part_file, f"{output_path}/inverted_index.txt")
            
            # Clean up temp directory
            import shutil
            shutil.rmtree(f"{output_path}_tmp")
        
        # Write a summary file with metadata
        summary_path = os.path.join(output_path, "_SUMMARY.txt")
        with open(summary_path, 'w') as f:
            f.write(f"Index created at: {output_path}\n")
            f.write(f"Source data: {input_path}\n")
            f.write(f"Stemming used: {use_stemming}\n")
            f.write(f"Total documents: {total_docs}\n")
            f.write(f"Total unique terms: {df_df.count()}\n")
            
        # Show some statistics
        print("Index statistics:")
        print(f"Total unique terms: {df_df.count()}")
        print(f"Total document-term pairs: {indexed_df.count()}")
        
        print("Job completed successfully!")
    
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()