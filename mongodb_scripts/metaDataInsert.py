from pymongo import MongoClient

# Connect with MongoDB
mongo_client = MongoClient("mongodb://127.0.0.1:27017")
print("Connection Successful")
# Create the database named "ir"
db = mongo_client.ir

print("Calculating collection statistics...")

# Method 1: Using aggregation with $count
N_result = list(db.invertedIndex.aggregate([
    {"$unwind": "$docIdList"},
    {"$group": {"_id": "$docIdList.docId"}},
    {"$count": "total_docs"}
]))

N = N_result[0]["total_docs"] if N_result else 0

# Calculate total length and get document lengths
doc_stats = list(db.invertedIndex.aggregate([
    {"$unwind": "$docIdList"},
    {"$group": {"_id": "$docIdList.docId", "doc_len": {"$first": "$docIdList.doc_len"}}}
]))

total_length = sum(d["doc_len"] for d in doc_stats)
avgdl = total_length / N if N > 0 else 0

print(f"Total number of unique documents: {N}")
print(f"Total length of all documents: {total_length}")
print(f"Average document length (avgdl): {avgdl:.2f}")

# Check if the collection already exists and drop it if needed
if "metaData" in db.list_collection_names():
    db.metaData.drop()
    print("Dropped existing 'metaData' collection")

# Create the metaData collection
meta_data = {
    "N": N,
    "avgdl": avgdl,
    "total_length": total_length
}

# Save the metadata to the collection
result = db.metaData.insert_one(meta_data)
print(f"Metadata saved successfully with ID: {result.inserted_id}")

# Verify the saved data
saved_meta = db.metaData.find_one()
print(f"Verified saved metadata: {saved_meta}")

mongo_client.close()
print("Connection closed")