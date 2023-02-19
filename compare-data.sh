# To send all output to log file, use
#   (bash compare-data.sh) &> compare-data.log


ID=AMPTECCE_H0_MEPA@0
PARAMETERS=SpinPeriod
# This START/STOP ...
START="1985-06-21T14:50:52Z"
STOP="1985-06-26T00:00:00Z"
# ... shows first and third records with timestamps
START="1985-06-25T16:15.47265097"
STOP="1985-06-25T16:15.47633562"
# but
START="1985-06-21T14:50:52.472Z"
STOP="1985-06-26T00:00:00.477Z"

ID=AC_H2_CRIS
PARAMETERS=flux_B
START="2010-06-21T14:50:52Z"
#STOP="2010-06-23T15:50:52Z"
STOP="2010-07-30T14:50:52Z"

#ID=OMNI_HRO_1MIN
#PARAMETERS=SYM_H
#START="2010-06-01T00:00:00Z"
#STOP="2010-07-01T00:00:00Z"

#Fails for both
#ID=A2_K0_MPA
#PARAMETERS=dens_lop
#START="2003-10-29T00:03:04Z"
#STOP="2003-10-31T00:03:04.000Z"

START_STR=${START//-/}
START_STR=${START_STR//:/}
STOP_STR=${STOP//-/}
STOP_STR=${STOP_STR//:/}

base="https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets"
url="$base/${ID//@*/}/data/$START_STR,$STOP_STR/$PARAMETERS?format=text"
#echo $url
#time curl -s $url > compare-data.cdas.csv

# Not requesting gzip does not change timing
#echo $url
#time node ../../server-nodejs/bin/CDAWeb.js \
#    --id ${ID//@*/} --parameters $PARAMETERS --start $START --stop $STOP \
#    --encoding '' > compare-data.cdas.csv

# HAPI server does not have ability to return gzipped
#time curl -s -H 'Accept-Encoding: gzip' $url | gunzip > compare-data.hapi.csv

#cmd="node -v"
# $cmd > a
TMPDIR=$TMPDIR/compare
mkdir $TMPDIR
ln -s $TMPDIR compare
rm -f $TMPDIR/compare-data.*

cmd="node CDAS2HAPIcsv.js \
--id ${ID//@*/} --parameters $PARAMETERS --start $START --stop $STOP \
--format csv --debug"
echo -e "\n"
echo "---- HAPI CSV via transform of CDAS text response ----"
echo "$cmd"
/usr/bin/time -ah $cmd > $TMPDIR/compare-data.cdas.csv
head -11 $TMPDIR/compare-data.cdas.csv


cmd="node CDAS2HAPIcsv.js \
--id ${ID//@*/} --parameters $PARAMETERS --start $START --stop $STOP \
--format text --debug"
echo -e "\n"
echo "---- CDAS text unaltered ----"
echo "$cmd"
/usr/bin/time -ah $cmd > $TMPDIR/compare-data.cdas.text
head -6 $TMPDIR/compare-data.cdas.text
grep $TMPDIR/compare-data.cdas.text -e "^[0-9]" | head -3


cmd="node CDAS2HAPIcsv.js \
--id ${ID//@*/} --parameters $PARAMETERS --start $START --stop $STOP \
--format cdf"
echo -e "\n"
echo "---- CDAS CDF unaltered ----"
echo "$cmd"
/usr/bin/time -ah $cmd > $TMPDIR/compare-data.cdas.cdf


url="https://cdaweb.gsfc.nasa.gov/hapi/data"
url="$url?id=$ID&parameters=$PARAMETERS&time.min=$START&time.max=$STOP"
echo -e "\n"
echo "---- HAPI CSV via production server ----"
echo "$url"
/usr/bin/time -ah curl -s "$url" > $TMPDIR/compare-data.hapi.csv
head -3 $TMPDIR/compare-data.hapi.csv


url="$url&format=binary"
echo -e "\n"
echo "---- HAPI Binary via production server ----"
echo "$url"
/usr/bin/time -ah curl -s "$url" > $TMPDIR/compare-data.hapi.bin


echo -e "\n"
echo "---- Response files ----"
ls -lh compare/compare-data*
