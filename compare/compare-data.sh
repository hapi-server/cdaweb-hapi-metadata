OUTDIR=data

CDASENCODING="" # or "gzip" (Does not change timing much)
# nl server does not have ability to return gzipped

if [ "$1" == "" ]; then
  $1=AC_H2_CRIS
fi
if [ "$2" == "" ]; then
  $2=flux_B
fi
if [ "$3" == "" ]; then
  $3="2010-06-21T14:50:52Z"
fi
if [ "$4" == "" ]; then
  $4="2010-07-30T14:50:52Z"
fi

ID=$1
PARAMETERS=$2
START=$3
STOP=$4

START_STR=${START//-/}
START_STR=${START_STR//:/}
STOP_STR=${STOP//-/}
STOP_STR=${STOP_STR//:/}

base="https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets"
url="$base/${ID//@*/}/data/$START_STR,$STOP_STR/$PARAMETERS?format=text"

mkdir -p $OUTDIR

cmd="node ../CDAS2HAPIcsv.js \
--id $ID --parameters $PARAMETERS --start $START --stop $STOP \
--format csv --encoding $CDASENCODING --debug"
echo -e "\n"
echo "---- HAPI CSV via transform of CDAS text response ----"
echo "$cmd"
/usr/bin/time -ah $cmd > $OUTDIR/$ID.cdas.csv
head -3 $OUTDIR/$ID.cdas.csv


cmd="node ../CDAS2HAPIcsv.js \
--id $ID --parameters $PARAMETERS --start $START --stop $STOP \
--format text --encoding $CDASENCODING --debug"
echo -e "\n"
echo "---- CDAS text unaltered ----"
echo "$cmd"
/usr/bin/time -ah $cmd > $OUTDIR/$ID.cdas.text
#head -6 $OUTDIR/$ID.cdas.text
grep $OUTDIR/$ID.cdas.text -e "^[0-9]" | head -3


cmd="node ../CDAS2HAPIcsv.js \
--id $ID --parameters $PARAMETERS --start $START --stop $STOP \
--format cdf --encoding $CDASENCODING"
echo -e "\n"
echo "---- CDAS CDF unaltered ----"
echo "$cmd"
/usr/bin/time -ah $cmd > $OUTDIR/$ID.cdas.cdf


url="https://cdaweb.gsfc.nasa.gov/hapi/data"
url="$url?id=$ID&parameters=$PARAMETERS&time.min=$START&time.max=$STOP"
echo -e "\n"
echo "---- HAPI CSV via production server ----"
echo "$url"
/usr/bin/time -ah curl -s "$url" > $OUTDIR/$ID.hapi.csv
head -3 $OUTDIR/$ID.hapi.csv


url="$url&format=binary"
echo -e "\n"
echo "---- HAPI Binary via production server ----"
echo "$url"
/usr/bin/time -ah curl -s "$url" > $OUTDIR/$ID.hapi.bin


echo -e "\n"
echo "---- Response files ----"
ls -lh $OUTDIR/$ID*
