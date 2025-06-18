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
    const collection = client.db('ir').collection('invertedIndex');
    const doc = await collection.findOne({});

    expect(doc).toHaveProperty('word');
    expect(doc).toHaveProperty('docIdList');
    expect(Array.isArray(doc.docIdList)).toBe(true);
    expect(doc.docIdList.length).toBeGreaterThan(0);

    const sampleEntry = doc.docIdList[0];
    expect(sampleEntry).toHaveProperty('docId');
    expect(sampleEntry).toHaveProperty('tf');
    expect(sampleEntry).toHaveProperty('df');
    expect(sampleEntry).toHaveProperty('doc_len');
    expect(sampleEntry).toHaveProperty('tfidf');

    await client.close();
});
