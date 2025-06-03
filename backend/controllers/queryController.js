
// === controllers/queryController.js ===
const { stemQuery } = require('../services/stemmingService');
const { getDocuments, getResultDocuments, getLuceneResults } = require('../services/mongoService');
const { addBm25 } = require('../services/scoringService');
const { getImageFilenames, getBase64Images } = require('../utils/fileUtils');
const { parseHrtimeToSeconds } = require('../utils/helpers');
const { MongoClient } = require('mongodb');

const URI = "mongodb://127.0.0.1:27017";

exports.processQuery = async (req, res) => {
    const startTime = process.hrtime();
    let query = (req.query.query || '').trim();
    const optionName = req.query.optionName;
    const scoringType = "tfidf"; // or "bm25"
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
    let fullbodyDocsList = [], chunkedBodyDocsList = [], invertedIndexList = [];

    try {
        const stemmedWords = await stemQuery(query);

        await client.connect();

        const [wordToDocMap, docToBm25MapSorted, invertedIndexList] = await getDocuments(client, stemmedWords);
        // invertedIndexList = indexList;

        [fullbodyDocsList, chunkedBodyDocsList] = await getResultDocuments(client, docToBm25MapSorted);
        const imageFileNames = getImageFilenames(fullbodyDocsList);
        
        const baseImagePath = "/home/tmaju002/Desktop/Workspace/github/Wiki_Search_Engine/CrawledData/storage/images"

        const imageResults = await getBase64Images(imageFileNames, baseImagePath);

        res.json({
            imageResult: imageFileNames,
            textResult: chunkedBodyDocsList,
            searchTime: parseHrtimeToSeconds(process.hrtime(startTime))
        });

    } catch (err) {
        console.error(err);
        res.status(500).send("Server Error");
    } finally {
        await client.close();
    }
};
