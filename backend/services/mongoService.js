// === services/mongoService.js ===
const fs = require('fs');

// === services/mongoService.js ===

// services/mongoService.js

async function getDocuments(client, stemmedWords, rankingMethod = 'tfidf') {
    const k1 = 1.5;
    const b = 0.75;

    const db = client.db('ir');

    // fetch meta + postings in parallel
    const [stats, indexEntries] = await Promise.all([
        db.collection('metaData').findOne({}, { projection: { _id: 0, N: 1, avgdl: 1 } }),
        db.collection('invertedIndex')
            .find(
                { word: { $in: stemmedWords } },
                { projection: { _id: 0, 'word': 1, 'docIdList.docId': 1, 'docIdList.tf': 1, 'docIdList.doc_len': 1 } }
            )
            .toArray(),
    ]);

    const N = stats.N;
    const avgdl = stats.avgdl;

    const scores = Object.create(null);

    for (let i = 0; i < indexEntries.length; i++) {
        const posting = indexEntries[i].docIdList;
        const df = posting.length;

        if (rankingMethod === 'bm25') {
            const idf = Math.log((N - df + 0.5) / (df + 0.5) + 1);
            for (let j = 0; j < posting.length; j++) {
                const { docId, tf, doc_len: dl } = posting[j];
                const numerator = tf * (k1 + 1);
                const denominator = tf + k1 * (1 - b + b * (dl / avgdl));
                scores[docId] = (scores[docId] || 0) + idf * (numerator / denominator);
            }
        } else {
            const idf = Math.log(N / df);
            for (let j = 0; j < posting.length; j++) {
                const { docId, tf } = posting[j];
                scores[docId] = (scores[docId] || 0) + tf * idf;
            }
        }
    }

        const docToScoreMapSorted = new Map(
        Object.entries(scores).sort((a, b) => b[1] - a[1])
    );

    // Only return what you use
    return docToScoreMapSorted;

}

// === services/mongoService.js ===
// Replace your current getResultDocuments with this
// NOTE: assumes docToScoreMapSorted is Map<file_id, score>

async function getResultDocuments(client, docToScoreMapSorted, topK = 50, bodySlice = null) {
    // 1) take topK ids and keep a score map to sort results later
    const topDocIds = [];
    const scoreMap = Object.create(null);
    for (const [docId, score] of docToScoreMapSorted) {
        topDocIds.push(docId);
        scoreMap[docId] = score;
        if (topDocIds.length === topK) break;
    }

    // 2) projection keeps payload small; add $slice on body if it's an array and you want fewer chunks
    const projection = {
        _id: 1,
        docId: 1,
        body: 1,
        filename: 1,
        url: 1,
        images: 1,
        file_id: 1,
    };

    let cursor = client.db('ir').collection('wikipedia').find(
        { file_id: { $in: topDocIds } },
        { projection }
    );

    // If you know body is an array of chunks and want only the first N chunks:
    // switch to an aggregation to $slice on the server:
    // if (bodySlice && Number.isInteger(bodySlice)) {
    //   cursor = client.db('ir').collection('wikipedia').aggregate([
    //     { $match: { file_id: { $in: topDocIds } } },
    //     { $project: { _id:1, docId:1, filename:1, url:1, images:1, file_id:1, body: { $slice: ['$body', bodySlice] } } }
    //   ]);
    // }

    const docs = await cursor.toArray();

    // 3) restore the desired order: highest score first
    docs.sort((a, b) => (scoreMap[b.file_id] || 0) - (scoreMap[a.file_id] || 0));

    // 4) build the lightweight list used by the UI
    const chunkedDataList = docs.map(doc => ({
        _id: doc._id,
        docId: doc.docId,
        chunkedBody: doc.body,
        filename: doc.filename,
        url: doc.url,
    }));

    return [docs, chunkedDataList];
}


module.exports = { getDocuments, getResultDocuments};
