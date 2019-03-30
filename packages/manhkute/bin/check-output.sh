#!/usr/bin/env bash
sid_group_n=$(hdfs dfs -count -q $1 |  awk '{print($5)}')
file_n=$(hdfs dfs -count -q $1 |  awk '{print($6)}')

if [[ $sid_group_n == $2 && $file_n == $3 ]]; then
  exit 0
else
  exit 1
fi