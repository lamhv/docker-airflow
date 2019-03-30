#!/usr/bin/env bash
# $1: file .py
# $2: date
# $3: start (sid_group)
# $4: mod

NEW_UUID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
mkdir /tmp/oozie-run-ds-code-$NEW_UUID
cd /tmp/oozie-run-ds-code-$NEW_UUID
sudo chmod 777 /tmp/oozie-run-ds-code-$NEW_UUID
sudo -u production /apps/anaconda2/bin/python $1 $2 $3 $4
