# Twitter Geocoder

Location information in  a tweet can be found in the following places

* Twitter Coordinates: Coordinates are reverse geocoded to a **\<country, state, city\>** tuple by finding the most populous city or second/third administrative division within **30km** of it. Note: This expansion is accepted with a confidence of 1.0

* Twitter Places: The country information extracted from this field agrees with the twitter coordinates field around **92%** times.
* User Location: The country information obtained from this field agrees with the coordinates field for about **95%** times. All words in this field is used for matching. No filtering based on word length is done for this field.
* User description: Matches with coordinates for about **14.5%** times. Also only words with length greater than 3 is used for matching.
* Tweet Body/Text: Only words with length ggreater than 3 are used for matching.Country extracted from tweet text matches with coordinates only for about **7%** times.

* Retweet User Location: Matches **57%** times.
* Retweet User Description: Matches **16.7%** times.


### Matching text with geonames gazetteer
1. An ElasticSearch server containing the geonames gazetteer is used for this purpose.
2. Only locations with population greater than 5000 or considered to be an administrative division (first, second or third order) or section of a populated city is used for matching.
3. The **name, asciiname and alternatenames** fields in the gazetteer are used for the matching.
4. All results(matches) obtained for a text query is associated with two scores, the product of which is used as the extraction confidence. The first score is the relevance score returned by elasticsearch, which is score calculated based on tf-idf (specifically okapi's BM25). The second score is calculated based on relative population size with respect to all returned matches for a given piece of text. The second score is needed because of ambiguous location names. Also if the text query matches any of the fields exactly then the relevance score is boosted by a factor of 2.0 for that particular match.
5. All text fields are split on **, - /** and each part is searched separately. If each part returns matches, then the number of matches are further filtered by considering only the intersection set.


### Location information fusion
Reference:  _Einat Amitay, Nadav Har'El, Ron Sivan, and Aya Soffer. 2004. Web-a-where: geotagging web content. In Proceedings of the 27th annual international ACM SIGIR conference on Research and development in information retrieval (SIGIR '04). ACM, New York, NY, USA, 273-280. DOI=http://dx.doi.org/10.1145/1008992.1009040_

Each location match of  **<country, state, city>** tuple is associated with a confidence score *p* as described in the above section, where it is assumed that the **city** name was mentioned in the text. The enclosing **country and state** are not mentioned directly in the text and are assigned a lower score of *p<sup>2</sup>d* & p<sup>2</sup>d<sup>2</sup> where d=0.7.
Now the scores thus obtained for each country, state or city from each field is summed up to get the final score for each expansion.
The final summed up scores for each country, state or city is then used to pick the most probable location expansion for each field(text, user location, user description ,.etc.)

### Geo-Focus
Finally, the geo-focus of the tweet is obtained by finding the location with maximum score. The score here is the product of location confidence (as described above)  and field confidence. The field confidence score is the percentage times a location extracted from that field matches with location extracted from twitter coordinates field at the country level.


##### Evaluation

1. Comparison with current embers-geocoder.
             
             **country-level**    **admin level**     **city-level**
              
|country name|embers | es-geo| embers| es-geo| embers| es-geo|NumTweets |
|------------|-------|-------|-------|-------|-------|-------|----------|
|Colombia    |0.86   |  0.90 |0.34   |  0.40 | 0.0   |  0.07 |10000     |
|Egypt       |0.61   |  0.97 |0.0    |  0.67 | 0.0   |  0.64 |10000     |



2. Evaluation for India, Kenya, Nigeria

|country name|Country-level  | State         | city          |NumTweets |
|------------| ------------- |:-------------:| -------------:|---------:|
|India       |0.97           |0.58           |0.40           | 10000    |
|Kenya       |0.95           |0.31           |0.82           |59602     |
|Nigeria     |0.96           |0.90           |0.58           |124830    |


