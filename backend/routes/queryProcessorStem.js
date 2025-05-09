var express = require('express');
const { MongoClient } = require('mongodb');
var router = express.Router();
const fs = require('fs');
const { type } = require('os');
const exec = require('child_process').exec;
const util = require('util');
const { response } = require('../app');
const readFile = util.promisify(fs.readFile);
const path = require('path');
const internal = require('stream');

const URI = "mongodb://127.0.0.1:27017";
const stemedQueryFile = "output.txt";

async function stemQuery(query) {
    const outputFilePath = "output.txt";
    const cmd = `java -jar TestIR-1.0-SNAPSHOT-jar-with-dependencies.jar "${query}" "${outputFilePath}"`;

    return new Promise((resolve, reject) => {
        exec(cmd, (error, stdout, stderr) => {
            if (error) {
                console.warn(error);
            }
            //console.log(stdout);
            console.log("Hello")
            resolve(stdout ? stdout : stderr);
        });
    });
}

async function getDocuments(client) {
    databasesList = await client.db().admin().listDatabases();

    const queryData = fs.readFileSync(stemedQueryFile);
    const queryWords = queryData.toString().split(" ");
    console.log(queryWords);

    const wordToDocMap = new Map();
    const docToTfidfMap = new Map();
    let invertedIndexList = [];
    for (i = 0; i < queryWords.length; i++) {
        const curWord = queryWords[i];
        let docDataList = [];
        // query in the invertedIndexStem collection
        // input of the query is: word
        // output of the query is the json entry of the word and it's document id list.
        // structure of the output is like this: [{ 'word': 'ucr', docIdList:[{docId: 'doc1', 'tfidf': '1'}]}]
        const invertedIndexEntries = await client.db('ir').collection('invertedIndexStem').find({ 'word': curWord }).toArray();
        //console.log(invertedIndexEntries);

        for (let i = 0; i < invertedIndexEntries.length; i++) {
            // const element = invertedIndexEntries[i];
            // query in the wikipedia collection
            // input of the query is: docId
            // output of the query is, e.g., [{'docId': 'ucr', 'body': 'jhweh', 'filename':'ucr.txt', 'url': 'https://w.com'}]

            // for (let j = 0; j < element.docIdList.length; j++) {
            //     const doc = element.docIdList[i];
            //     const docData = await client.db('ir').collection('wikipedia').find({'docId': doc.docId}).toArray();
            //     docDataList = docDataList.concat(docData);
            // }
            invertedIndexList = invertedIndexList.concat(invertedIndexEntries[i]);
            docDataList = docDataList.concat(invertedIndexEntries[i].docIdList);
        }

        for (let j = 0; j < docDataList.length; j++) {
            const tfIdf = docToTfidfMap.get(docDataList[j].docId);
            if (typeof tfIdf == "undefined") {
                docToTfidfMap.set(docDataList[j].docId, docDataList[j].tfIdf);
            } else {
                const newTfIdf = parseFloat(tfIdf) + parseFloat(docDataList[j].tfIdf)
                docToTfidfMap.set(docDataList[j].docId, newTfIdf);
            }
        }

        wordToDocMap.set(curWord, docDataList);
        //fs.writeFileSync("test.json", JSON.stringify(docDataList, null, 4));
    }

    // sort the docToTfifdMap by value
    const docToTfidfMapSorted = new Map([...docToTfidfMap.entries()].sort((a, b) => b[1] - a[1]));

    //console.log(wordToDocMap);
    return [wordToDocMap, docToTfidfMapSorted, invertedIndexList];
};


function cutTheArticle(docData) {
    //console.log(docData);
    //console.log(docData);
    let chunkedData = []
    for (let i = 0; i < docData.length; i++) {
        let body = docData[i].body;
        let chunkedBody = body.slice(0, Math.min(3, body.length));
        let chunkedString = "";
        let counter = 0;
        for (let j = 0; j < body.length; j++) {
            for (let k = 0; k < body[j].length; k++) {
                chunkedString += body[j][k];
                if (body[j][k] == ".") {
                    counter++;
                }
                if (counter == 2) {
                    break
                }
            }
            if (counter == 2) {
                break;
            }
        }

        //console.log(chunkedBody[1].length);
        // console.log(chunkedBody);
        let chunkedElement = {
            "_id": docData[i]._id,
            "docId": docData[i].docId,
            "chunkedBody": chunkedString,
            "filename": docData[i].filename,
            "url": docData[i].url,
        }
        chunkedData.push(chunkedElement);
    }
    return chunkedData;
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
        const docData = await client.db('ir').collection('wikipedia').find({ 'docId': docId }).toArray();
        //console.log(docData);
        const chunkedDocData = cutTheArticle(docData);
        docDataList = docDataList.concat(docData);
        chunkedDataList = chunkedDataList.concat(chunkedDocData);
        i++;
        if (i == 20) {
            break;
        }
    }
    return [docDataList, chunkedDataList];
}

function getLuceneDocNameList(query) {
    const fileData = fs.readFileSync("luceneResults/shakespeareSearch.txt");
    const lines = fileData.toString().split("\n");
    let start =  false;
    let docIdList = [];

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];

        if (line.includes("TF-IDF score:")) {
            start = true;
        }
        if (start) {
            try {
                const splitWords = line.split(")-->");
                let docIdInitial = splitWords[1];
                console.log(docIdInitial);
                let docId = docIdInitial;
                console.log(docId);
                let tfIdfPart = splitWords[0];
                let rank = tfIdfPart.split("( score:")[0];
                let tfIdf = tfIdfPart.split("( score:")[1];
                docIdList.push(docId);
                if (parseInt(rank) >= 7) {
                    start = false;
                }
            }
            catch(err) {
                console.log(err);
            }
        }
    }
    return docIdList;
}

async function getLuceneResults(client, query) {
    let docDataList = [];
    let chunkedDataList = [];
    let i = 0;

    let docIdList = getLuceneDocNameList(query);
    console.log(docIdList);

    for (let i = 0; i < docIdList.length; i++) {
        // const element = invertedIndexEntries[i];
        // query in the wikipedia collection
        // input of the query is: docId
        // output of the query is, e.g., [{'docId': 'ucr', 'body': 'jhweh', 'filename':'ucr.txt', 'url': 'https://w.com'}]
        const docId = docIdList[i];
        const docData = await client.db('ir').collection('wikipedia').find({ 'docId': docId }).toArray();
        //console.log(docData);
        const chunkedDocData = cutTheArticle(docData);
        docDataList = docDataList.concat(docData);
        chunkedDataList = chunkedDataList.concat(chunkedDocData);
        if (i == 20) {
            break;
        }
    }
    return [docDataList, chunkedDataList];
}

function getImageFilenames(chunkedBodyDocsList) {
    let fileNameList = [];
    for (let i = 0; i < chunkedBodyDocsList.length; i++) {
        try {
            const docId = chunkedBodyDocsList[i].docId;
            fileNamePart = docId.replace("_", " ");
            fileNameList.push(fileNamePart);
        } catch (err) {
            console.log(err);
        }
    }
    return fileNameList;
}

function getWordCount(textArray) {
    let wordCnt = 0;
    for (let i = 0; i < textArray.length; i++) {
        let wordsCntInLine = textArray[i].split(" ").length;
        wordCnt += wordsCntInLine;
    }
    return wordCnt;
}

function BM25_score(query, docId, data, wordCountDoc) {
    var k1 = 1.2;
    var k2 = 100;
    var b = 0.75;
    // var avLen = 3094; //average number of words in all documents
    var avLen = wordCountDoc;
    var totalFiles = 12521;
    var totalWords = 38755637;
    var K = k1 * ((1 - b) + b * (0.9));
    const words = query.split(" ");
    //console.log(words);

    var bm25 = 0.0;
    for (var i = 0; i < words.length; i++) {
        //console.log(words[i]);
        var word = words[i];
        let score_array = data.find(element => element.word === words[i]).docIdList;
        //console.log(scoress);
        let docObject = score_array.find(element => element.docId === docId);
        var tfIdf = 0.0;
        if (typeof (docObject) != "undefined") {
            tfIdf = docObject.tfIdf;
        }
        //console.log(tfIdf);
        let idf = totalFiles / score_array.length;
        let n_i = score_array.length;
        //console.log(n_i);
        let f_i = Math.ceil(tfIdf / idf);
        //finding qf_i
        var qf_i = 0;
        for (var j = 0; j < words.length; j++) {
            if (words[j] == word) {
                qf_i = qf_i + 1;
            }
        }
        //console.log(qf_i);

        bm25 = bm25 + Math.log((totalFiles - n_i + 0.5) / (n_i + 0.5)) * (((k1 + 1) * f_i) / (K + f_i)) * (((k2 + 1) * qf_i) / (k2 + qf_i));
    }
    return bm25;
}

function addBm25(fullbodyDocsList, chunkedBodyDocsList, invertedIndexList, query) {
    console.log("addbm25", invertedIndexList);
    let docToBM25Map = new Map();
    for (let i = 0; i < fullbodyDocsList.length; i++) {
        const wordCountDoc = getWordCount(fullbodyDocsList[i].body);
        const bm25 = BM25_score(query, fullbodyDocsList[i].docId, invertedIndexList, wordCountDoc);
        docToBM25Map.set(fullbodyDocsList[i].docId, bm25);
        console.log(bm25);
    }
    const sortedDocToBM25Map = new Map([...docToBM25Map.entries()].sort((a, b) => b[1] - a[1]));
    let newChunkedBodyDocsList = [];
    for (let [key, value] of sortedDocToBM25Map) {
        newChunkedBodyDocsList.push(chunkedBodyDocsList.find(element => element.docId === key))
    }
    return newChunkedBodyDocsList;
}

function parseHrtimeToSeconds(hrtime) {
    var seconds = (hrtime[0] + (hrtime[1] / 1e9)).toFixed(3);
    return seconds;
}


/* GET home page. */
router.get('/', async function (req, res, next) {
    var startTime = process.hrtime();
    let query = req.query.query;
    const searchType = req.query.searchType;
    const optionName = req.query.optionName;
    const scoringType = "tfidf";
    console.log(typeof query == "undefined");
    console.log(req.query);

    if (typeof query == "undefined") {
        return res.send({
            "success": false,
            "result": []
        })
    } else if (query.trim().length == "") {
        return res.send({
            "success": false,
            "result": []
        })
    }

    query = query.trim();

    console.log("req.params: check");
    console.log(req.query);
    const client = new MongoClient(URI);
    let docList = []
    let wordToDocMap;
    let docToTfidfMap;
    let docToScoreMapSorted;
    let resultDocsList;
    try {
        // stem the query
        await stemQuery(query);
        //connect the database
        await client.connect()

        // get documents for the query words
        // the output is a map <word, doc>
        let result = [];
        result = await getDocuments(client);

        wordToDocMap = result[0];
        docToScoreMapSorted = result[1];
        const invertedIndexList = result[2];
        // load top 5 documents
        if (optionName === "hadoop") {
            resultDocsList = await getResultDocuments(client, docToScoreMapSorted);
            fullbodyDocsList = resultDocsList[0];
            chunkedBodyDocsList = resultDocsList[1];
        } else {
            resultDocsList = await getLuceneResults(client, query);
            fullbodyDocsList = resultDocsList[0];
            chunkedBodyDocsList = resultDocsList[1];
        }

        // add bm25 results 
        if (scoringType === "bm25") {
            chunkedBodyDocsList = addBm25(fullbodyDocsList, chunkedBodyDocsList, invertedIndexList, query);
        }

        console.log("New era of beginning");

        //console.log(docToTfidfMap);
        //console.log(docToTfidfMapSorted);
        //console.log(wordToDocMap);
        // console.log(result.get('ucr').length)
        // console.log(result.get('soccer').length)
    } catch (err) {
        console.log(err);
    }
    //client.close()
    //console.log(chunkedBodyDocsList);
    // get the image files
    let imageFileNames = getImageFilenames(chunkedBodyDocsList);
    var fileNames = [];
    const basePath = "/Users/sakibfuad/Documents/winter2022/IR/project/data/crawledImages";

    fileNames = fs.readdirSync(basePath, ['**.*']);  // use async function instead of sync

    let outputFileNames = [];
    for (let i = 0; i < imageFileNames.length; i++) {
        for (let j = 0; j < fileNames.length; j++) {
            if (fileNames[j].includes(imageFileNames[i])) {
                outputFileNames.push(fileNames[j]);
            }
        }
    }

    var data = {};
    const files = outputFileNames.map(function (filename) {
        filepath = path.join(basePath, filename);
        return readFile(filepath); //updated here
    });

    const response = {};
    Promise.all(files).then(fileNames => {
        //response.data = fileNames;
        let result = [];
        for (let i = 0; i < fileNames.length; i++) {
            var b64 = Buffer.from(fileNames[i]).toString('base64')
            result.push(b64);
        }
        response.imageResult = result;
        response.textResult = chunkedBodyDocsList;
        response.searchTime = parseHrtimeToSeconds(process.hrtime(startTime));
        res.json(response);
    }).catch(error => {
        res.status(400).json(response);
    });
});

module.exports = router;
