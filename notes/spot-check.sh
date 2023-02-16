# Use
#   (bash spot-check.sh) &> spot-check.log
# to send all output to log file

ID=AC_H2_CRIS
PARAMETERS=flux_B
START="2010-06-21T14:50:52Z"
#STOP="2010-06-23T15:50:52Z"
STOP="2010-07-30T14:50:52Z"

ID=AMPTECCE_H0_MEPA@0
PARAMETERS=SpinPeriod
# This START/STOP
START="1985-06-21T14:50:52Z"
STOP="1985-06-26T00:00:00Z"
# shows first and third records with timestamps
#START="1985-06-25T16:15:47265097"
#STOP="1985-06-25T16:15:47633562"


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
#time curl -s $url > spot-check.cdas.csv

# Not requesting gzip does not change timing
#echo $url
#time node ../../server-nodejs/bin/CDAWeb.js \
#    --id ${ID//@*/} --parameters $PARAMETERS --start $START --stop $STOP \
#    --encoding '' > spot-check.cdas.csv

# HAPI server does not have ability to return gzipped
#time curl -s -H 'Accept-Encoding: gzip' $url | gunzip > spot-check.hapi.csv

rm -f /tmp/spot-check.*

cmd="node ../../server-nodejs/bin/CDAWeb.js \
--id ${ID//@*/} --parameters $PARAMETERS --start $START --stop $STOP \
--format csv --debug"
echo "----"
echo "$cmd"
time $cmd > /tmp/spot-check.cdas.csv
head -11 /tmp/spot-check.cdas.csv


cmd="node ../../server-nodejs/bin/CDAWeb.js \
--id ${ID//@*/} --parameters $PARAMETERS --start $START --stop $STOP \
--format text --debug"
echo "----"
echo "$cmd"
time $cmd > /tmp/spot-check.cdas.text
head -6 /tmp/spot-check.cdas.text
grep /tmp/spot-check.cdas.text -e "^[0-9]" | head -3

exit

cmd="node ../../server-nodejs/bin/CDAWeb.js \
--id ${ID//@*/} --parameters $PARAMETERS --start $START --stop $STOP \
--format cdf"
echo "----"
echo "$cmd"
time $cmd > /tmp/spot-check.cdas.cdf


url="https://cdaweb.gsfc.nasa.gov/hapi/data"
url="$url?id=$ID&parameters=$PARAMETERS&time.min=$START&time.max=$STOP"
echo "----"
echo "$url"
time curl -s "$url" > /tmp/spot-check.hapi.csv
head -3 /tmp/spot-check.hapi.csv


url="$url&format=binary"
echo "----"
echo "$url"
time curl -s "$url" > /tmp/spot-check.hapi.bin


ls -lh /tmp/spot-check*


