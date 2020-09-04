import nltk
import os
import re

loc_default = {"korea": "south korea", "u.s.": "united states", "us": "united states"}
blacklist = ["member states", "city", "", "united nation", "united nations"]
administrativeNames = ['president', 'country', 'governorate', 'administrative',
                       'district', 'division', 'town', 'village', 'of']

adminRegex = re.compile(r'\b' + r'\b|\b'.join(administrativeNames))

with open(os.path.join(os.path.dirname(__file__), 'stopwords.txt')) as inf:
    stop_words = set([l.strip() for l in inf])


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


def remove_administrativeNames(text):
    return adminRegex.sub('', text).strip()
