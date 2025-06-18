
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
// const stemmer = require('porter-stemmer').stemmer;

// exports.stemQuery = async (query) => {
//     const words = query.trim().split(/\s+/);
//     const stemmedWords = words.map(w => stemmer(w.toLowerCase()));
//     return stemmedWords; // returns an array of stemmed words
// };

// services/tokenizationService.js
const { spawn } = require('child_process');

exports.stemQuery = async (query) => {
    return new Promise((resolve, reject) => {
        const py = spawn('python3', ['utils/tokenizeQuery.py']);

        let output = '';
        let error = '';

        py.stdout.on('data', (data) => {
            output += data.toString();
        });

        py.stderr.on('data', (data) => {
            error += data.toString();
        });

        py.on('close', (code) => {
            if (code !== 0) {
                return reject(`Python process exited with code ${code}\n${error}`);
            }
            try {
                const tokens = JSON.parse(output);
                resolve(tokens);  // returns array of preprocessed words
            } catch (err) {
                reject(`Failed to parse Python output: ${output}`);
            }
        });

        py.stdin.write(query + '\n');
        py.stdin.end();
    });
};
