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
python2 initialize.py
rm -rf gnames_gaz
