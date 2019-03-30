#!/usr/bin/env bash

usage()
{
    echo "Usage: convert.sh -p <path> -t <path> -d <path>"
    echo "-p | --path           : folder that contains .ipynb files"
    echo "-t | --target_path    : target folder that will contain .py files"
    echo "-d | --dag_path       : dag target path that will contain .json files"
    echo "-h | --help           : to help"
}

while [ "$1" != "" ]; do
    case $1 in
        -p | --path )           shift
                                paths=$1
                                ;;
        -t | --target_path )    shift
                                target_path=$1
                                ;;
        -d | --dag_path )       shift
                                dag_path=$1
                                ;;
        -h | --help )           usage
                                exit
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done


rm -rf $dag_path/*.json

OIFS=$IFS
IFS=','
mails2=$paths
for x in $mails2
do
    ls -l $x | grep .ipynb | awk -v path="$x" -v dag_path="$dag_path" -v target_path="$target_path" '
    {
        system("python parse_ipynb/parse_ipynb.py " path "/" $9 " " target_path " " dag_path)
    }'
done

IFS=$OIFS

scp *.json admin@172.30.250.13:/dfs/data/airflow/dags/config/

#sh convert-folder.sh -p /Users/manh/Documents/notebooks/ds,/Users/manh/Documents/notebooks/pysparks -t ~/Documents/notebooks/ds -d ~/Documents/notebooks/ds

#sh bin/convert.sh -p /home/production/working/core_dev/cs_dev/lib_nb_exp,/home/production/working/core_dev/monitor_dev/lib_nb_exp -t /home/production/working/core_dev/cs_dev/lib_nb_converted/converted/ -d /home/production/working/core_dev/cs_dev/lib_nb_converted
