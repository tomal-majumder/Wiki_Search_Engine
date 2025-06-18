const { MongoClient, ServerApiVersion } = require('mongodb');
require('dotenv').config();
URI = process.env.MONGODB_URI;

test('reads a sample document from "invertedIndex" collection', async () => {
    const client = new MongoClient(URI, {
    serverApi: {
        version: ServerApiVersion.v1,
        strict: true,
        deprecationErrors: true,
    }
    });
    await client.connect();
    const collection = client.db('ir').collection('wikipedia');
    const doc = await collection.findOne({});

    expect(doc).toHaveProperty('docId');
    expect(doc).toHaveProperty('body');
    expect(doc).toHaveProperty('filename');
    expect(doc).toHaveProperty('file_id');
    expect(doc).toHaveProperty('url');

    // Check image array
    expect(doc).toHaveProperty('images');
    expect(Array.isArray(doc.images)).toBe(true);
    if (doc.images.length > 0) {
        const img = doc.images[0];
        expect(img).toHaveProperty('image_id');
        expect(img).toHaveProperty('image_path');
    }

    expect(doc).toHaveProperty('image_count');

    await client.close();
});
