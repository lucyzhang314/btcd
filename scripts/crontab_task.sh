#!/bin/sh

BTCD_PROGRAM=/Users/andy/dev/chainData/btcd
BTCD_DATADIR=/Users/andy/dev/chainData/data.btcd.console
DBTOOL_PROGRAM=/Users/andy/dev/chainData/dbtool

GITHUB_REPO_NAME=
GITHUB_REPO_PATH=

# function of start BTCD
function startBTCD() {
  echo "---------------------------------------------"
  echo "restart BTCD program ......"
  nohup $BTCD_PROGRAM --datadir=$BTCD_DATADIR --listen=0.0.0.0:9876 --rpclisten=0.0.0.0:9877 --connect=192.168.0.7:8333 &
}

# function of stop BTCD
# function stopBTCD() {
#   echo "---------------------------------------------"
#   echo "stop BTCD program for gracefully shutting down the database ......"
#   ps -ef | grep $BTCD_PROGRAM | grep grep -v | awk '{print $2}' | xargs kill

#   # wait for BTCD shutdown completely
#   count=`ps -ef | grep $BTCD_PROGRAM | grep grep -v | wc -l`
#   while (( count > 0 ))
#   do
#     sleep 5 # sleep to wait for BTCD shutdown gracefully
#     count=`ps -ef | grep $BTCD_PROGRAM | grep grep -v | wc -l`
#     if (( count > 0 )); then
#       echo "Waiting for BTCD shutdown completely ......"
#     fi
#   done
#   echo "BTCD shutdown completely"
# }

# function of stop BTCD
stopBTCD() {
  echo "---------------------------------------------"
  echo "stop BTCD program for gracefully shutting down the database ......"

  ps -ef | grep btcd | grep grep -v | awk '{print $2}' | xargs kill -s TERM

  # wait for BTCD shutdown completely
  count=`ps -ef | grep btcd | grep grep -v | wc -l`
  echo $count
  while test $count -ne 0
  do
    sleep 5 # sleep to wait for BTCD shutdown gracefully
    count=`ps -ef | grep $BTCD_PROGRAM | grep grep -v | wc -l`
    echo $count
    if test $count -ne 0 
    then
      echo "Waiting for BTCD shutdown completely ......"
    fi
  done
  echo "BTCD shutdown completely"
}


# kill BTCD
stopBTCD


# generate temp dir name
cur_dateTime="`date +%Y%m%d%H%m%s`"
tempDirToBeDump="./tmp"$cur_dateTime"_toBeDump"
tempDirNew="./tmp"$cur_dateTime"_toBeDeliver"
echo $tempDirToBeDump
echo $tempDirNew

# backup BTCD data dir
echo "---------------------------------------------"
echo "backup BTCD data dir to a temporary dir ......"
cp -rv $BTCD_DATADIR $tempDirToBeDump

# restart BTCD
startBTCD

# start to dump BTCD latest status
echo "---------------------------------------------"
echo "dump BTCD data ......"
$DBTOOL_PROGRAM dump $tempDirToBeDump $tempDirNew

# remove temporary directory
echo "---------------------------------------------"
echo "remove temporary directory ......"
rm -rf $tempDirToBeDump

# send $tempDirNew dir to BT for share on internet
# clone public seed on github
sleep 5
git clone $GITHUB_REPO_PATH
cd $GITHUB_REPO_NAME

#-------------------------------------------------------------------------------
# CALL bt functions to generate BT-Seeds here
# ---------------------
#-------------------------------------------------------------------------------

# commit to github
git commit -a -m "generate new BT seed"
git push origin
