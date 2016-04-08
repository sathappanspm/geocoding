#!/bin/bash

if [ -f /home/embers/.embers ]; then
  . /home/embers/.embers
fi

PYTHONPATH="/home/embers/embers/bin"
HOSTNAME=`hostname`

export PYTHONPATH
export CLUSTERNAME

python /home/embers/embers/bin/etool/cache/cache.py --hostname $HOSTNAME --log_level ERROR

