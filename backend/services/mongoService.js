// === services/mongoService.js ===
const fs = require('fs');

async function getDocuments(client, stemmedWords, rankingMethod = 'tfidf') {
    const wordToDocMap = new Map();
    const docToScoreMap = new Map();
    let invertedIndexList = [];

    // BM25 constants
    const k1 = 1.5;
    const b = 0.75;

    const stats = await client.db('ir').collection('metaData').findOne({});
    const N = stats.N;
    const avgdl = stats.avgdl;

    // Batch query: fetch all words in one go
    const indexEntries = await client.db('ir')
        .collection('invertedIndex')
        .find({ word: { $in: stemmedWords } })
        .toArray();

    for (const entry of indexEntries) {
        const curWord = entry.word;
        const df = entry.docIdList.length;
        const idf = Math.log((N - df + 0.5) / (df + 0.5) + 1);

        let docDataList = [];

        for (const doc of entry.docIdList) {
            const tf = doc.tf;
            const dl = doc.doc_len;

            let score = 0;
            if (rankingMethod === 'bm25') {
                const numerator = tf * (k1 + 1);
                const denominator = tf + k1 * (1 - b + b * (dl / avgdl));
                score = idf * (numerator / denominator);
            } else {
                score = tf * Math.log(N / df);
            }

            const prev = docToScoreMap.get(doc.docId) || 0;
            docToScoreMap.set(doc.docId, prev + score);
            docDataList.push(doc);
        }

        wordToDocMap.set(curWord, docDataList);
        invertedIndexList.push(entry);
    }

    const docToScoreMapSorted = new Map([...docToScoreMap.entries()].sort((a, b) => b[1] - a[1]));
    return [wordToDocMap, docToScoreMapSorted, invertedIndexList];
}

async function getResultDocuments(client, docToScoreMapSorted) {
    const topDocIds = Array.from(docToScoreMapSorted.keys()).slice(0, 20);

    const docData = await client.db('ir')
        .collection('wikipedia')
        .find({ file_id: { $in: topDocIds } })
        .toArray();

    const chunkedDataList = docData.map(doc => ({
        _id: doc._id,
        docId: doc.docId,
        chunkedBody: doc.body,
        filename: doc.filename,
        url: doc.url
    }));

    return [docData, chunkedDataList];
}

module.exports = { getDocuments, getResultDocuments};
