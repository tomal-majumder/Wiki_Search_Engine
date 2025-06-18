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
