
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://tmaju002:iqnT2P1pmChIIOtr@cluster0.duieh6r.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
    print("Failed to connect to MongoDB.")

# List all databases
databases = client.list_database_names()
print("\n📂 Databases:")

for db_name in databases:
    print(f" - {db_name}")
    db = client[db_name]
    
    # List all collections in the database
    collections = db.list_collection_names()
    print(f"   📁 Collections:")
    for coll_name in collections:
        coll = db[coll_name]
        count = coll.count_documents({})
        stats = db.command("collstats", coll_name)
        print(f"    - {coll_name}: {count} documents, {stats['size'] / 1024:.2f} KB")
# Close the client connection
client.close()
# Note: Ensure that the MongoDB URI is correct and that you have the necessary permissions to access the database.
# This script connects to a MongoDB Atlas cluster, lists all databases and their collections,