CREATE VIRTUAL TABLE geoname_full using fts3(geonameid int, name text collate nocase, asciiname text collate nocase, admin1 text collate nocase, country text collate nocase, population float, latitude real, longitude real, featureCode varchar(10), featureClass char(1), cc2 char(2), admin1Code varchar(20) collate nocase
);

INSERT INTO geonames_full SELECT g.id, g.name, g.asciiname, a.name, c.country, g.population, g.latitude, g.longitude, g.featureCOde, g.featureClass FROM allcities g, alladmins a, allcountries c where g.countryCode = c.ISO AND g.countryCode||'.'||g.admin1 = a.key


