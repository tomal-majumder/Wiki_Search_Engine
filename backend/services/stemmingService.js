
// === services/stemmingService.js ===
// const { exec } = require('child_process');
// const util = require('util');
// const execAsync = util.promisify(exec);

// exports.stemQuery = async (query) => {
//     const cmd = `java -jar TestIR-1.0-SNAPSHOT-jar-with-dependencies.jar "${query}" "output.txt"`;
//     try {
//         const { stdout } = await execAsync(cmd);
//         return stdout;
//     } catch (err) {
//         console.error('Stemming error:', err);
//         throw err;
//     }
// };

// services/stemmingService.js
const stemmer = require('porter-stemmer').stemmer;

exports.stemQuery = async (query) => {
    const words = query.trim().split(/\s+/);
    const stemmedWords = words.map(w => stemmer(w.toLowerCase()));
    return stemmedWords; // returns an array of stemmed words
};