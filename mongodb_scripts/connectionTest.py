from pymongo import MongoClient

# Connect to MongoDB
mongo_client = MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=2000)
mongo_client.admin.command('ping')  # check connection
print("✅ Connected to MongoDB")

# List all databases
databases = mongo_client.list_database_names()
print("\n📂 Databases:")
for db_name in databases:
    print(f" - {db_name}")
    db = mongo_client[db_name]
    
    # List all collections in the database
    collections = db.list_collection_names()
    print(f"   📁 Collections:")
    for coll_name in collections:
        coll = db[coll_name]
        count = coll.count_documents({})
        stats = db.command("collstats", coll_name)
        print(f"    - {coll_name}: {count} documents, {stats['size'] / 1024:.2f} KB")
