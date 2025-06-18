const { MongoClient, ServerApiVersion } = require('mongodb');

require('dotenv').config();
URI = process.env.MONGODB_URI;
test('connects to MongoDB successfully', async () => {
    const client = new MongoClient(URI, {
    serverApi: {
        version: ServerApiVersion.v1,
        strict: true,
        deprecationErrors: true,
    }
    });
    await expect(client.connect()).resolves.toBeDefined();
    await client.close();
});