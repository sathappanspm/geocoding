import nltk
import os

loc_default={"korea": "south korea"}
blacklist=["member states", "city", ""]
with open(os.path.join(os.path.dirname(__file__), 'stopwords.txt')) as inf:
    stop_words = set([l.strip().decode("utf-8") for l in inf])

def remove_stopwords(text):
    tokens = nltk.tokenize.wordpunct_tokenize(text)
    start = 0
    for idx, t in enumerate(tokens):
        if t in stop_words and start == idx:
            start += 1
        else:
            break
    
    return " ".join(tokens[start:])


def reduce_stopwords(text):
    for word in stop_words:
        if text.startswith(word):
            text = text.replace(word, '', 1).strip()
    return text.strip()
         
