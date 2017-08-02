#!/bin/bash

[ -e /etc/sysconfig/mvkg ] && . /etc/sysconfig/mvkg

RANDOM_SLEEP=${RANDOM_SLEEP:-"0"}
OMIT_JVM_STATS=${OMIT_JVM_STATS:-"false"}

SCRIPT=`readlink -f $0`
SCRIPT_PATH=`dirname $SCRIPT`

if [ "xx$ENVIRONMENT" = "xx" ]
then
  echo ENVIRONMENT must be set
  exit 1
fi

if [ "xx$APP_NAME" = "xx" ]
then
  APP_NAME="kafka"
fi

if [ "xx$KAFKA_HOST" = "xx" ]
then
  echo KAFKA_HOST must be set
  exit 1
fi

if [ "xx$KAFKA_PORT" = "xx" ]
then
  echo KAFKA_PORT must be set
  exit 1
fi

if [ "xx$CARBON_HOST" = "xx" ]
then
  echo CARBON_HOST must be set
  exit 1
fi

if [ "xx$CARBON_PORT" = "xx" ]
then
  echo CARBON_PORT must be set
  exit 1
fi

if [ "xx$HOSTNAME" = "xx" ]
then
  HOSTNAME=`hostname`
fi

OUTPUT=`python $SCRIPT_PATH/mvkg.py $HOSTNAME $ENVIRONMENT $APP_NAME $KAFKA_HOST $KAFKA_PORT $OMIT_JVM_STATS`

if [ $? = 0 ]
then
  if [ "$RANDOM_SLEEP" -gt "0" ]
  then
    # Sleep for random period of time
    R=$RANDOM
    R=$(( R %= RANDOM_SLEEP ))
    sleep $R
  fi
  echo "${OUTPUT}" | nc -w 20 $CARBON_HOST $CARBON_PORT
fi
