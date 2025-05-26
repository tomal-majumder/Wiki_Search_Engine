
// === utils/helpers.js ===
function parseHrtimeToSeconds(hrtime) {
    return (hrtime[0] + hrtime[1] / 1e9).toFixed(3);
}

function getWordCount(textArray) {
    return textArray.reduce((sum, line) => sum + line.split(" ").length, 0);
}

module.exports = { parseHrtimeToSeconds, getWordCount };