// services/mongoClient.js
const { MongoClient, ServerApiVersion } = require('mongodb');
require('dotenv').config(); // Load environment variables

const uri = process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017';
let clientPromise;

async function getClient() {
  if (!clientPromise) {
    const client = new MongoClient(uri, {
      serverApi: { version: ServerApiVersion.v1, strict: true, deprecationErrors: true },
      maxPoolSize: 20, // tune if needed 
      // why use maxPoolSize? Answer: It limits the number of concurrent connections to the database, which can help manage resource usage and prevent overwhelming the database server.
    });

    // optional: visibility into query durations
    // why use these event listeners? 
    // Answer: They provide insights into the performance of database operations, helping to identify slow queries or issues in the application.
    client.on('commandSucceeded', (e) => {
      if (['find', 'aggregate'].includes(e.commandName)) {
        console.log(`[Mongo OK] ${e.commandName} in ${e.duration}ms :: ${e.address}`);
      }
    });
    client.on('commandFailed', (e) => {
      console.warn(`[Mongo FAIL] ${e.commandName} after ${e.duration}ms`, e?.failure || '');
    });

    clientPromise = client.connect().then(() => client);
  }
  return clientPromise;
}

module.exports = { getClient };
