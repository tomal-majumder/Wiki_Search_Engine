#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, explode, col, lit, regexp_replace, udf
from pyspark.sql.types import ArrayType, StringType
import os, re, argparse

# --- Import NLP tools ---
import nltk
nltk.download('punkt', quiet=True)
from nltk.stem import PorterStemmer
import spacy
nlp = spacy.load("en_core_web_sm")


# --- TEXT CLEANING & TOKENIZATION ---

def clean_text(text):
    """Lowercase and remove non-alphanumeric characters except spaces."""
    if text is None:
        return ""
    return re.sub(r'[^a-zA-Z0-9\s]', '', text.lower())

def extract_meaningful_tokens(text, use_stemming=True):
    """
    Extract meaningful tokens from text with dual indexing:
    - Preserve named entities (PERSON, ORG, GPE) as complete phrases
    - ALSO index individual words from those entities
    - Apply Porter stemming to remaining non-entity tokens
    - Filter out junk long numbers (e.g., '00000') but keep short ones (e.g., '2021')
    """
    if not text:
        return []
    
    doc = nlp(text)
    stemmer = PorterStemmer()
    tokens = []
    entity_words = set()  # Track words that are part of entities

    # 1. Extract named entities and their components
    for ent in doc.ents:
        if ent.label_ in {"PERSON", "ORG", "GPE"}:
            full_entity = ent.text.strip().lower()
            
            # Add the complete entity
            tokens.append(full_entity)
            
            # Add individual words from the entity
            words_in_entity = full_entity.split()
            for word in words_in_entity:
                clean_word = re.sub(r'[^a-zA-Z0-9]', '', word)
                if len(clean_word) > 1:  # Skip single characters
                    tokens.append(clean_word)
                    entity_words.add(clean_word)  # Track to avoid double-processing

    # 2. Extract remaining tokens (skip ones already processed as entity components)
    for token in doc:
        word = token.text.strip().lower()
        clean_word = re.sub(r'[^a-zA-Z0-9]', '', word)
        
        if (not clean_word or clean_word in entity_words or 
            token.is_punct or token.is_space or token.is_stop):
            continue
            
        if clean_word.isdigit() and len(clean_word) > 4:
            continue  # remove junky numeric tokens
            
        # Apply stemming to non-entity words
        processed_word = stemmer.stem(clean_word) if use_stemming else clean_word
        tokens.append(processed_word)

    return tokens


# --- SPARK UDF BUILDER ---

def build_udfs(use_stemming):
    @udf(ArrayType(StringType()))
    def preprocess_udf(text):
        cleaned = clean_text(text)
        return extract_meaningful_tokens(cleaned, use_stemming)
    return preprocess_udf


# --- MAIN SPARK PIPELINE ---

def main():
    parser = argparse.ArgumentParser(description='Create an inverted index with dual NER indexing and stemming')
    parser.add_argument('input_path', help='Directory containing .txt files')
    parser.add_argument('--output', default='index_output', help='Output directory')
    parser.add_argument('--use-stemming', action='store_true', help='Apply Porter stemming to non-proper nouns')
    parser.add_argument('--format', default='parquet', choices=['parquet', 'csv', 'json', 'text'], help='Output format')
    parser.add_argument('--show-stats', action='store_true', help='Show detailed indexing statistics')
    args = parser.parse_args()

    spark = SparkSession.builder.appName("InvertedIndexDualNER").getOrCreate()
    preprocess = build_udfs(args.use_stemming)

    # Step 1: Read files and extract filename
    df = spark.read.text(args.input_path).withColumn("filename", input_file_name())
    df = df.withColumn("tokens", preprocess(col("value")))
    df = df.withColumn("filename", regexp_replace(col("filename"), "^.*/", ""))

    # Step 2: Explode and clean tokens
    words_df = df.select("filename", explode(col("tokens")).alias("term")) \
                 .filter((col("term").isNotNull()) & (col("term") != ""))

    # Step 3: TF, DF, Doc Length
    tf_df = words_df.groupBy("term", "filename").count().withColumnRenamed("count", "tf")
    doc_len_df = words_df.groupBy("filename").count().withColumnRenamed("count", "doc_len")
    df_df = words_df.distinct().groupBy("term").count().withColumnRenamed("count", "df")
    total_docs = df.select("filename").distinct().count()

    # Step 4: Join and compute TF-IDF
    indexed_df = tf_df.join(doc_len_df, "filename").join(df_df, "term") \
                      .withColumn("tfidf", col("tf") * (lit(total_docs) / col("df"))) \
                      .select("term", "filename", "tf", "df", "doc_len", "tfidf")

    # Step 5: Write output
    if args.format == "parquet":
        indexed_df.write.mode("overwrite").parquet(args.output)
    elif args.format == "csv":
        indexed_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(args.output)
    elif args.format == "json":
        indexed_df.coalesce(1).write.mode("overwrite").json(args.output)
    elif args.format == "text":
        indexed_df.coalesce(1).write.mode("overwrite").option("header", "true") \
                  .csv(f"{args.output}_tmp", sep="\t")
        import glob, shutil
        os.makedirs(args.output, exist_ok=True)
        part_file = glob.glob(f"{args.output}_tmp/part-*")[0]
        os.rename(part_file, f"{args.output}/inverted_index.txt")
        shutil.rmtree(f"{args.output}_tmp")

    # Step 6: Show statistics
    unique_terms = df_df.count()
    print(f"✅ Index created at {args.output}")
    print(f"📄 Total documents: {total_docs}")
    print(f"🧠 Unique terms: {unique_terms}")
    
    if args.show_stats:
        print("\n📊 Detailed Statistics:")
        print(f"   • Total term-document pairs: {tf_df.count()}")
        print(f"   • Average terms per document: {words_df.count() / total_docs:.1f}")
        
        # Show sample of multi-word entities vs single words
        sample_terms = indexed_df.select("term").distinct().limit(20).collect()
        multi_word_terms = [row.term for row in sample_terms if ' ' in row.term]
        if multi_word_terms:
            print(f"   • Sample multi-word entities: {multi_word_terms[:5]}")

    spark.stop()

if __name__ == "__main__":
    main()