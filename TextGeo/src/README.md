### Requirements
`pip install -r requirements.txt`
Elasticsearch 5.4

### Setup
Run `./setup.sh`

### TEST
`python2 geocode.py --infile tests/test_input.json --output tests/test_output.json`

OR 

```
ipython
>>> from geocode import BaseGeo
>>> from geoutils.dbManager import ESWrapper
>>> db = ESWrapper('geonames', 'places')
>>> geo = BaseGeo(db=db)
>>> geo.geocode_fromList(["US", "Arlington", "Virginia"], [])[0]
```
here the array passed to geocode_fromList function is the set on named entities detected as location from text. This can be obtained from sources like stanfordNLP, spacy, BasisTech etc.
