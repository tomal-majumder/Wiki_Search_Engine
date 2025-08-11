const natural = require('natural');

// Tokenizer + Porter stemmer (fast & common in IR systems)
const tokenizer = new natural.WordTokenizer();
const stemmer = natural.PorterStemmer;

// Basic normalization: lowercase + strip punctuation (keeps numbers/letters/spaces)
function normalize(text) {
  return text.replace(/[^A-Za-z0-9\s]/g, ' ').toLowerCase();
}

// Exported API identical to before (async + returns array of stems)
exports.stemQuery = async (raw) => {
  const tokens = tokenizer.tokenize(normalize(raw));
  // If you want to drop tiny tokens: tokens = tokens.filter(t => t.length > 1);
  return tokens.map(t => stemmer.stem(t));
};

