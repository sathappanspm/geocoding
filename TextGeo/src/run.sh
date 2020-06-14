FILES=`ls /home/sathap1/gmu_colombia_2016/x*`
BASEDIR="/home/sathap1/gmu_colombia_2016/"
for i in $FILES
do
    fname=`basename $i`
    python2 ./geocode.py --infile $i --outfile "${BASEDIR}/geocoder_$fname"
done
