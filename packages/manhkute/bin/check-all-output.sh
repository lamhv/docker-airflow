#!/usr/bin/env bash
check()
{
  root_dir="/data/working/prod_v00"
  path=$1
  nos=$2
  nof=$3
  day=$4
  sid_group_n=$(hdfs dfs -count -q $root_dir""$path""$4 |  awk '{print($5)}')
  file_n=$(hdfs dfs -count -q $root_dir""$path""$4 |  awk '{print($6)}')

  if [[ $sid_group_n == $nos && $file_n == $nof ]]; then
    return 0
  else
    return 1
  fi
}

while [[ 1 == 1 ]]; do
  sum=0
  while IFS='' read -r line || [[ -n "$line" ]]; do
    IFS=',' read -ra ADDR <<< "$line"
    check ${ADDR[0]} ${ADDR[1]} ${ADDR[2]} $2
    if [[ $? == 1 ]]; then
      sum=`expr $sum + 1`
      break
    fi
  done < "$1"
  if [[ $sum == 0 ]]; then
    exit 0
  fi
  sleep 5s
done