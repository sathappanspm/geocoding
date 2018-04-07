Data Files:
===========
A. World Gazetteer data file:
-----------------------------
- World-Gazetteer v1.0:
  - the original download from the website
- World-Gazetteer v2.0 
  - extra lat-lon points added for each city record by Graham@CACI using yahoo API
- World-Gazetteer v3.0 
  - re-compiled v1.0 and 2.0, added missing entries from v1.0 
  - To distinguish between extra lat-lon points added in v2.0 and original v1.0 entries,
    an extra column 'pad' is appended. '0' indicates original v1.0 entry and '1' is new
    padded lat-lon point. They have they same row id
  - A new entry had to be added to this data file, started this entry at id value = 1000000000
  - For all future additions of new entries id counter value should be 
    incremented +1 from 1000000000.

B. Latin American Countries and Admin1 data file
-----------------------------------------------
- lac_co_admin v1.0
  - Source: manually collected from wikipedia
  - Contains information on only 10 OSI specified countries
  - For each country/admin1 contains following information
     - ISO 3166 code
     - lat-lon: which corresponds to capital city of the country/admin1

Geocode version 0.1.1
=====================
Features:
----------
1. Uses world-gazetteer data for resolving location in tweets
2. Uses regular expressions based string matching
3. Fast lookups with KDTree data-structure using lat-lon
4. If tweet is not geotagged with lat-lon or places information, geocoding falls back on:
   user's location string, description and tweet text
5. Uses world-gazetteer version 2.0
6. Normalizes tweet payload in to a flat dictionary
7. Main API Calls:
   - geo_normalize
   - best_guess
   - lookup_city
8. Known Bugs/drawbacks:
   a. lat-lon was switched when reading from world-gazetteer, and switched again when entering
         information in coordinates data for KDTree
   b. aliases for cities were ignored
   c. spatial index structure used keys as <city, country> which overwrites duplicates cities in
      diff admins of same countries
   d. repeted matching of same lcoation string with city, country, admin reg. expressions
      lead to unwanted side-effect, where city names same as the parent country would
      get matched.
   
Geocode version 0.2.0 (builds on features from previous version 0.1.1)
======================================================================
Features:
----------
1.  Uses 2 data files's:
     - World-gazetteer (version 3.0)
     - lac_co_admin    (version 1.0)
2.  Country & City Alias aware
     - Uses city aliases as specified in world-gazetteer. 
     - Also added were (a few) informal aliases for cities which were learned manually
       from randomly sampled tweets during testing
     - Country specific aliases from wikipedia and google, which correspond to aliases
       used in regional languages/dialects
3.  Country & Admin1 ABBR/CODE aware
      - The ISO 3166 code's are collected from wikipedia
      - So far includes this information for only the 10 OSI specified countries
5.  Returns or resolves a city when only country and admin1 are known, where it 
    picks the most populous city in list of possible matches
6.  Returns or resolves a city when only country and (optionaly) admin1 are known, and if 
    they city and admin1 share the same name
7.  Returns admin1 level geocoding if city cannot be identified from matching
    text/code but admin1 and county name have been identified
8.  Returns country level geocoding if neither admin1 or city can be identified but
    country is identified
9.  Uses tweet text to identify country only if neither city, admin1 or country can be
    identified from previous two passes of matching full names or codes on user's location string
10. Resolved geocode by doing multi-passes over different geocodable peices of information in
    tweet. In a specific order of priority 
11. API CHANGE LOG:
      a. User facing API function names remain same
      b. return parameter for geo_normalize: 
         <city, country, admin1, admin2, admin3, population, lat, lon, id, pad>
      c. Input argument 'user' for geo_normalize has been removed
      d. tweet payload normalization requirement is removed
           IMP: Do not use 'utils.payload_normalize' in conjunction with geo_normalize
      e. input args modified to <best_guess(self, city=None, admin1=None, country=None)>
12. Known Bugs/Drawbacks:
      a. Since it also uses tweet's text or user profile description text for geocoding
         causes (unknown) side-effects which can incorrect at city and admin1 level, not
         so much for country level
      b. Heuristic approach to send parital geocodable information at admin/country level
         when either the city or admin cannot be resolved leads to incorrect result
      c. Use of admin or country code matching leads incorrect results in special cases
         such as U.S will match some admin of same code 

 
IMP NOTE:
---------
Since we use user's location string as a primary fall-back in order to geo-locate
a tweet (if its not already geo-tagged with lat-lon and places information):

It is possible that the tweet actual location (from where it was tweeted from) might not
correspond to the User's mentioned location in his/her profile. And, In fact we see that 
from experiments we have performed on data-sift twitter data. In such experiments, we collect
tweets that are geo-tagged (containing lat-lon) and use that as ground-truth and run our 
geocoding scripts on the same set of tweets with lat-lon geotag removed. And in results,
we observed a lot of mismatches between the ground truth and location result that was 
returned from our geocoding scripts.

TODO List
---------
1.  Remove 'Bs As' as alias for Buenos Aires from world-gazetteer data file
        - Instead create a separate database file for such informal aliases for cities
2.  Instead add short alias abbreviations such as "Bs.as", "Bs As", "Cba", "CD"
    to dictionary in either code file or sep. dat file and then add
    that to alias list. But maybe there should be a separate logic as well
    since they can match any combination of chars in desc
3.  Develop logic and Analyze results if tz & utcoffset fields
    of tweet can be useful in geolocation
4.  Logic for fetching results through reverse lookup, when only
    city is present 
5.  Extract all places and lat-lon information from twitter, and build a separate
    world-gazetteer like data file -- partially complete
       5.1 - Using bounding boxes from twitter places, and replace KDTree lookup with RTree lookup
       5.2 - Update world-gazetteer data with more lat-lon data points from twitter places & geo tags
6.  Collect more aliases in common or informal use for countries, cities, and admin
7.  Incorporate geo-location mining with tweet's text, using
       -  hashtags
       -  key-phrases
       -  landmarks
       7.1 - Create a new (world-gazetteer like) database of (latin-american) landmarks mentioned
             in tweets, and funnel that in to geocoding process

