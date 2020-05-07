curl -X GET "localhost:9200/geonames/_search?pretty" -H 'Content-Type:application/json' -d '{"query": {"bool": {"must": {"match_all": {}}, "filter": [{"geo_distance": {"distance": "30km", "coordinates": [-77.06, 38.5249]}}, {"terms": {"featureClass": ["a", "h", "l", "t", "p", "v"]}}]}}, "sort": {"population": "desc"}, "size": 1}'

