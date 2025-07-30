// === controllers/queryController.js ===
const { stemQuery } = require('../services/stemmingService');
const { getDocuments, getResultDocuments} = require('../services/mongoService');
const { getImageFilenames} = require('../utils/fileUtils');
const { parseHrtimeToSeconds } = require('../utils/helpers');
const { MongoClient, ServerApiVersion } = require('mongodb');
require('dotenv').config(); // Load environment variables

const URI = process.env.MONGODB_URI;

exports.processQuery = async (req, res) => {
    const startTime = process.hrtime();
    let query = (req.query.query || '').trim();
    const scoringType = (req.query.optionName || 'tfidf').toLowerCase();
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

    // const client = new MongoClient(URI);
    // Create a MongoClient with a MongoClientOptions object to set the Stable API version
    const client = new MongoClient(URI, {
    serverApi: {
        version: ServerApiVersion.v1,
        strict: true,
        deprecationErrors: true,
    }
    });
    let fullbodyDocsList = [], chunkedBodyDocsList = [], invertedIndexList = [];

    try {
        const stemmedWords = await stemQuery(query);

        await client.connect();

        const [wordToDocMap, docToBm25MapSorted, invertedIndexList] = await getDocuments(client, stemmedWords, scoringType);
        // invertedIndexList = indexList;

        [fullbodyDocsList, chunkedBodyDocsList] = await getResultDocuments(client, docToBm25MapSorted);
        const imageFileNames = getImageFilenames(fullbodyDocsList);
        
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
