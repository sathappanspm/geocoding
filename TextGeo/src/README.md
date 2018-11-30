### Requirements
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
