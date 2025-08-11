// === controllers/queryController.js ===
const { stemQuery } = require('../services/stemmingService');
const { getDocuments, getResultDocuments } = require('../services/mongoService');
const { getImageFilenames } = require('../utils/fileUtils');
const { parseHrtimeToSeconds } = require('../utils/helpers');
const { getClient } = require('../services/mongoClient');
const { startSpan, sysSnapshot } = require('../utils/profiler');

require('dotenv').config();

exports.processQuery = async (req, res) => {
    const startTime = process.hrtime();
    const measures = [];
    const endTotal = startSpan('total_request', measures);

    // validate input
    const endValidate = startSpan('validate_input', measures);
    let query = (req.query.query || '').trim();
    const scoringType = (req.query.optionName || 'tfidf').toLowerCase();

    if (!query || query.length === 0) {
    endValidate();
    endTotal();
    return res.status(400).json({ success: false, result: [], error: 'Empty query' });
    }
    endValidate();

    // pooled Mongo client (already connected)
    const endConnectToDB = startSpan('connect_to_db', measures);
    const client = await getClient();
    endConnectToDB();

    let fullbodyDocsList = [], chunkedBodyDocsList = [];

    try {
    const endStem = startSpan('stem_query', measures);
    const stemmedWords = await stemQuery(query);
    endStem();

    const endGetDocs = startSpan('get_documents', measures);
    const docToScoreMapSorted = await getDocuments(client, stemmedWords, scoringType);
    endGetDocs();

    const endFetchResults = startSpan('fetch_results', measures);
    [fullbodyDocsList, chunkedBodyDocsList] = await getResultDocuments(client, docToScoreMapSorted);
    endFetchResults();

    const endGetImages = startSpan('get_image_filenames', measures);
    const imageFileNames = getImageFilenames(fullbodyDocsList);
    endGetImages();

    endTotal();

    res.json({
        imageResult: imageFileNames,
        textResult: chunkedBodyDocsList,
        searchTime: parseHrtimeToSeconds(process.hrtime(startTime)),
        profile: { measures, sysSnapshot: sysSnapshot() },
    });
    } catch (err) {
        console.error(err);
        endTotal();
        res.status(500).send('Server Error');
    }
};
