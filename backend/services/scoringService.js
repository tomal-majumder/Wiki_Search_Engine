// === services/scoringService.js ===
const { getWordCount } = require('../utils/helpers');

function BM25_score(query, docId, data, wordCount) {
    const k1 = 1.2, k2 = 100, b = 0.75;
    const totalDocs = 12521;
    const words = query.split(" ");
    const K = k1 * ((1 - b) + b * (wordCount / 3094));

    let bm25 = 0;
    for (const word of words) {
        const entry = data.find(e => e.word === word);
        if (!entry) continue;
        const docEntry = entry.docIdList.find(d => d.docId === docId);
        const tf = docEntry ? docEntry.tfIdf : 0;
        const n = entry.docIdList.length;
        const idf = Math.log((totalDocs - n + 0.5) / (n + 0.5));
        bm25 += idf * (((k1 + 1) * tf) / (K + tf)) * (((k2 + 1)) / (k2 + 1));
    }
    return bm25;
}

function addBm25(fullDocs, chunkedDocs, indexList, query) {
    const map = new Map();
    for (const doc of fullDocs) {
        const score = BM25_score(query, doc.docId, indexList, getWordCount(doc.body));
        map.set(doc.docId, score);
    }
    const sorted = [...map.entries()].sort((a, b) => b[1] - a[1]);
    return sorted.map(([id]) => chunkedDocs.find(d => d.docId === id));
}

module.exports = { addBm25 };
