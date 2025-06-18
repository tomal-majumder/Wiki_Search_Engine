#!/usr/bin/env python3
import sys, json, re
import nltk
nltk.download('punkt', quiet=True)
from nltk.stem import PorterStemmer
import spacy
nlp = spacy.load("en_core_web_sm")

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9\s]', '', text.lower())

def extract_meaningful_tokens(text):
    doc = nlp(text)
    stemmer = PorterStemmer()
    tokens = []
    entity_words = set()

    # Add named entities and their parts
    for ent in doc.ents:
        if ent.label_ in {"PERSON", "ORG", "GPE"}:
            entity = ent.text.strip().lower()
            tokens.append(entity)
            for word in entity.split():
                clean_word = re.sub(r'[^a-zA-Z0-9]', '', word)
                if len(clean_word) > 1:
                    tokens.append(clean_word)
                    entity_words.add(clean_word)

    # Add stemmed non-entity words
    for token in doc:
        word = token.text.strip().lower()
        clean_word = re.sub(r'[^a-zA-Z0-9]', '', word)
        if (not clean_word or clean_word in entity_words or 
            token.is_punct or token.is_space or token.is_stop):
            continue
        if clean_word.isdigit() and len(clean_word) > 4:
            continue
        tokens.append(stemmer.stem(clean_word))

    return tokens

if __name__ == "__main__":
    query = sys.stdin.read().strip()
    cleaned = clean_text(query)
    token_list = extract_meaningful_tokens(cleaned)
    print(json.dumps(token_list))
