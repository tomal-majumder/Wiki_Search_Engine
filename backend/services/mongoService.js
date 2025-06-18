
// === services/mongoService.js ===
const fs = require('fs');

// async function getDocuments(client, stemmedWords) {
//     // Map to store which documents each word appears in
//     const wordToDocMap = new Map();
//     // Map to accumulate TF-IDF scores for each document
//     const docToTfidfMap = new Map();
//     // List to collect all inverted index entries found
//     let invertedIndexList = [];


//     // Iterate over each stemmed word from the query
//     for (const curWord of stemmedWords) {
//         let docDataList = [];
//         // Query the inverted index collection for the current word
//         const invertedIndexEntries = await client.db('ir')
//             .collection('invertedIndex')
//             .find({ 'word': curWord })
//             .toArray();

//         // For each entry, collect document lists and store the entry
//         for (const entry of invertedIndexEntries) {
//             invertedIndexList.push(entry);
//             docDataList = docDataList.concat(entry.docIdList);
//         }

//         // For each document, sum up the TF-IDF scores
//         for (const doc of docDataList) {
//             const prev = docToTfidfMap.get(doc.docId) || 0;
//             docToTfidfMap.set(doc.docId, prev + parseFloat(doc.tfIdf));
//         }

//         // Map the current word to its associated documents
//         wordToDocMap.set(curWord, docDataList);
//     }

//     // Sort documents by their total TF-IDF score in descending order
//     const docToTfidfMapSorted = new Map([...docToTfidfMap.entries()].sort((a, b) => b[1] - a[1]));
//     // Return the word-to-doc map, sorted doc-to-tfidf map, and all inverted index entries found
//     return [wordToDocMap, docToTfidfMapSorted, invertedIndexList];
// }

async function getDocuments(client, stemmedWords, rankingMethod = 'tfidf') {
    const wordToDocMap = new Map();
    const docToScoreMap = new Map();
    let invertedIndexList = [];

    // Constants for BM25
    const k1 = 1.5;
    const b = 0.75;

    const stats = await client.db('ir').collection('metaData').findOne({});
    const N = stats.N;
    const avgdl = stats.avgdl;

    for (const curWord of stemmedWords) {
        let docDataList = [];

        const invertedIndexEntries = await client.db('ir')
            .collection('invertedIndex')
            .find({ word: curWord })
            .toArray();

        for (const entry of invertedIndexEntries) {
            invertedIndexList.push(entry);
            const df = entry.docIdList.length;

            // IDF for both TF-IDF and BM25
            const idf = Math.log((N - df + 0.5) / (df + 0.5) + 1); // standard BM25 IDF

            for (const doc of entry.docIdList) {
                const tf = doc.tf;
                const dl = doc.doc_len;
                let score = 0;

                if (rankingMethod === 'bm25') {
                    const numerator = tf * (k1 + 1);
                    const denominator = tf + k1 * (1 - b + b * (dl / avgdl));
                    score = idf * (numerator / denominator);
                } else {
                    // Default: TF-IDF
                    score = tf * Math.log(N / df);
                }

                const prevScore = docToScoreMap.get(doc.docId) || 0;
                docToScoreMap.set(doc.docId, prevScore + score);

                docDataList.push(doc);
            }
        }

        wordToDocMap.set(curWord, docDataList);
    }

    const docToScoreMapSorted = new Map([...docToScoreMap.entries()].sort((a, b) => b[1] - a[1]));

    return [wordToDocMap, docToScoreMapSorted, invertedIndexList];
}

async function getResultDocuments(client, docToScoreMapSorted) {
    let docDataList = [];
    let chunkedDataList = [];
    let i = 0;

    for (let [key, value] of docToScoreMapSorted) {
        // const element = invertedIndexEntries[i];
        // query in the wikipedia collection
        // input of the query is: docId
        // output of the query is, e.g., [{'docId': 'ucr', 'body': 'jhweh', 'filename':'ucr.txt', 'url': 'https://w.com'}]
        console.log(key + " = " + value);
        const docId = key;
        const docData = await client.db('ir').collection('wikipedia').find({ 'file_id': docId }).toArray();
        //console.log(docData);
        //const chunkedDocData = cutTheArticle(docData);
        chunckedData = []
        for (let i = 0; i < docData.length; i++) {
            let chunkedElement = {
                "_id": docData[i]._id,
                "docId": docData[i].docId,
                "chunkedBody": docData[i].body,
                "filename": docData[i].filename,
                "url": docData[i].url,
            }
            chunckedData.push(chunkedElement);
        }

        docDataList = docDataList.concat(docData);
        chunkedDataList = chunkedDataList.concat(chunckedData);
        i++;
        if (i == 20) {
            break;
        }
    }
    return [docDataList, chunkedDataList];
}

module.exports = { getDocuments, getResultDocuments};
