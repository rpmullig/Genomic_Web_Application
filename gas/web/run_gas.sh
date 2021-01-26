#!/bin/bash

# run_gas.sh
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Rns the GAS app using the Gunicorn server for production environments
#
##

cd /home/ubuntu/cp-rpmullig/gas/web
source /home/ubuntu/cp-rpmullig/gas/web/.env
[[ -d /home/ubuntu/cp-rpmullig/gas/web/log ]] || mkdir /home/ubuntu/cp-rpmullig/gas/web/log
if [ ! -e /home/ubuntu/cp-rpmullig/gas/web/log/$GAS_LOG_FILE_NAME ]; then
    touch /home/ubuntu/cp-rpmullig/gas/web/log/$GAS_LOG_FILE_NAME;
fi
if [ "$1" = "console" ]; then
    LOG_TARGET=-
else
    LOG_TARGET=/home/ubuntu/cp-rpmullig/gas/web/log/$GAS_LOG_FILE_NAME
fi
/home/ubuntu/.virtualenvs/mpcs/bin/gunicorn \
  --log-file=$LOG_TARGET \
  --log-level=debug \
  --workers=$GUNICORN_WORKERS \
  --certfile=/usr/local/src/ssl/ucmpcs.org.crt \
  --keyfile=/usr/local/src/ssl/ucmpcs.org.key \
  --bind=$GAS_APP_HOST:$GAS_HOST_PORT gas:app
