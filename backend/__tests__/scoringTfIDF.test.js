const { getDocuments } = require('../services/mongoService');
const { MongoClient, ServerApiVersion } = require('mongodb');
require('dotenv').config();
URI = process.env.MONGODB_URI;
test('BM25 scores are returned in sorted order', async () => {
    const client = new MongoClient(URI, {
    serverApi: {
        version: ServerApiVersion.v1,
        strict: true,
        deprecationErrors: true,
    }
    });
    await client.connect();
    const tokens = ['messi', 'argentina']
    const [_, scoreMap] = await getDocuments(client, tokens, 'tfidf');
    const scores = Array.from(scoreMap.values());

    expect(scores.length).toBeGreaterThan(1);

    for (let i = 0; i < scores.length - 1; i++) {
        expect(scores[i]).toBeGreaterThanOrEqual(scores[i + 1]);
    }
    await client.close();
});
