import csv
import os
import json
from pymongo import MongoClient
from collections import defaultdict

# Connect with MongoDB
mongo_client = MongoClient("mongodb://127.0.0.1:27017")
print("Connection Successful")
# Create the database named "ir"
db = mongo_client.ir

# File path - change it according to your machine
file_path = "/home/tmaju002/Desktop/Workspace/Projects/Wiki_Search_Engine/IndexData/inverted_index.csv"

# Check if the collection already exists and drop it if needed
if "invertedIndex" in db.list_collection_names():
    db.invertedIndex.drop()
    print("Dropped existing 'invertedIndex' collection")

# Dictionary to store terms and their associated documents
term_dict = defaultdict(list)

# Process the CSV file
if os.path.isfile(file_path):
    try:
        # First pass: group by term
        print("Reading CSV file and grouping by term...")
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            # Skip header row
            next(csvfile)
            
            csv_reader = csv.reader(csvfile)
            line_count = 0
            
            for row in csv_reader:
                if len(row) >= 6:  # Ensure the row has enough columns
                    term = row[0]
                    filename = row[1]
                    tf = float(row[2])
                    df = int(row[3])
                    doc_len = int(row[4])
                    tfidf = float(row[5])
                    
                    # Create a document entry
                    doc_entry = {
                        "docId": filename.split('.txt')[0],  # Remove .txt extension
                        "tf": tf,
                        "df": df,
                        "doc_len": doc_len,
                        "tfidf": tfidf
                    }
                    
                    # Add to the term's document list
                    term_dict[term].append(doc_entry)
                    
                    line_count += 1
                    if line_count % 10000 == 0:
                        print(f"Processed {line_count} lines...")
                else:
                    print(f"Warning: Skipping malformed row: {row}")
        
        print(f"Processed a total of {line_count} lines.")
        print(f"Found {len(term_dict)} unique terms.")
        
        # Second pass: insert into MongoDB
        print("Inserting terms into MongoDB...")
        inserted_count = 0
        
        for term, doc_list in term_dict.items():
            data = {
                "word": term,
                "docIdList": doc_list,
                "document_count": len(doc_list)
            }
            
            # MongoDB collection name for invertedIndex data is "invertedIndex"
            rec_id = db.invertedIndex.insert_one(data)
            
            inserted_count += 1
            if inserted_count % 1000 == 0:
                print(f"Inserted {inserted_count} terms...")
        
        print(f"Successfully inserted {inserted_count} terms into the database.")
        
        # Create an index on the "word" field for faster queries
        print("Creating index on 'word' field...")
        db.invertedIndex.create_index("word")
        print("Index created successfully.")
        
    except Exception as e:
        print(f"An exception occurred: {str(e)}")

else:
    print(f"File not found: {file_path}")

print("Processing complete")
mongo_client.close()