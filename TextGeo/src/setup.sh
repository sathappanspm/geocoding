CUR_DIR=`pwd`

if [ -d "gnames_gaz" ]
then
    rm -rf gnames_gaz
fi

mkdir gnames_gaz ;
cd gnames_gaz;

wget http://download.geonames.org/export/dump/admin2Codes.txt
wget http://download.geonames.org/export/dump/admin1CodesASCII.txt
wget http://download.geonames.org/export/dump/countryInfo.txt
wget http://download.geonames.org/export/dump/allCountries.zip
unzip allCountries.zip
cd $CUR_DIR
curl -XPUT 'localhost:9200/geonames' -H 'Content-Type: application/json' -d @../data/es_settings.json
python initialize.py
#rm -rf gnames_gaz
