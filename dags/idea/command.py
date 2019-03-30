from idea.const import *

verify_score_task = """
sudo -u hdfs spark-submit \
--class com.trustingsocial.applications.CalculatePSI \
--name "Calculate PSI for {{ next_execution_date.strftime('%Y%m%d') }} with ref:20180513" \
--master yarn-cluster \
--driver-cores 2 \
--driver-memory 1g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=1g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 4 \
--executor-memory 4g \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--ref-path {ref_path} \
--act-path {act_path} \
--hdfs-uri hdfs://trustiq \
--accepted-path {accepted_path} \
--accepted-psi {accepted_psi}""".format(ref_path=reference_path,
                                        act_path=active_score_path.format("{{ next_execution_date.strftime('%Y%m%d') }}"),
                                        accepted_path=accepted_path.format("{{ next_execution_date.strftime('%Y%m%d') }}"),
                                        accepted_psi="0.01")

score_encrypted = """
sudo -u hdfs spark-submit \
--class com.trustingsocial.telkomsel.starter.UtilStart \
--name "encryted-score-{}" \
--master yarn-cluster \
--driver-cores 2 \
--driver-memory 1g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=1g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 4 \
--executor-memory 5g \
hdfs://trustiq/apps/libs/ts-telco-telkomsel-assembly-1.0.jar \
idea_encryt_credit_score \
{} \
{}
""".format("{{ next_execution_date.strftime('%Y%m%d') }}",
           active_score_path.format("{{ next_execution_date.strftime('%Y%m%d') }}"),
           score_lead_gen_encrypted_path.format("{{ next_execution_date.strftime('%Y%m%d') }}"))

merged_location = """
sudo -u hdfs spark-submit \
--class com.trustingsocial.telkomsel.starter.UtilStart \
--name "merge-location-{}" \
--master yarn-cluster \
--driver-cores 2 \
--driver-memory 1g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=1g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 4 \
--executor-memory 5g \
hdfs://trustiq/apps/libs/ts-telco-telkomsel-assembly-1.0.jar \
idea_merge_location \
{} \
{}
""".format("{{ next_execution_date.strftime('%Y%m%d') }}",
           location_path.format("{{ next_execution_date.strftime('%Y%m%d') }}"),
           merged_location_path.format("{{ next_execution_date.strftime('%Y%m%d') }}"))

create_subscriber = """
sudo -u hdfs spark-submit \
--class com.trustingsocial.telkomsel.starter.UtilStart \
--name "create-subscribers-{}" \
--master yarn-cluster \
--driver-cores 2 \
--driver-memory 1g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=1g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 4 \
--executor-memory 5g \
hdfs://trustiq/apps/libs/ts-telco-telkomsel-assembly-1.0.jar \
idea_create_subscriber \
{} \
{} \
{} \
{}
""".format("{{ next_execution_date.strftime('%Y%m%d') }}",
           score_lead_gen_encrypted_path.format("{{ next_execution_date.strftime('%Y%m%d') }}"),
           merged_location_path.format("{{ next_execution_date.strftime('%Y%m%d') }}"),
           tags_path.format("{{ next_execution_date.strftime('%Y%m%d') }}"),
           subscribers_path.format("{{ next_execution_date.strftime('%Y%m%d') }}"))

create_tags = """
ssh -t admin@172.30.250.1 "cd /home/admin/scripts && sudo -u jupyter sh run_generate_tags.sh && exit $?" 
"""

load_subscribers = """
sudo -u hdfs spark-submit \
--class com.trustingsocial.telkomsel.starter.UtilStart \
--name "load-subscribers-{}" \
--master yarn-cluster \
--driver-cores 2 \
--driver-memory 1g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=2g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 6 \
--executor-memory 8g \
hdfs://trustiq/apps/libs/ts-telco-telkomsel-assembly-1.0.jar \
load_into_db \
{} \
jdbc:mariadb://172.30.250.53:3308?sessionVariables=sql_mode='ANSI_QUOTES' \
bigdata \
bigdata@123 \
subscriber_manager \
subscribers_new \
3200 \
subscriber_id,encrypted_credit_score,encrypted_locations,tags
""".format("{{ next_execution_date.strftime('%Y%m%d') }}",
           subscribers_path.format("{{ next_execution_date.strftime('%Y%m%d') }}"))

load_credit_score = """
sudo -u hdfs spark-submit \
--class com.trustingsocial.telkomsel.starter.UtilStart \
--name "load-subscribers-{}" \
--master yarn-cluster \
--driver-cores 2 \
--driver-memory 1g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=2g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 6 \
--executor-memory 10g \
hdfs://trustiq/apps/libs/ts-telco-telkomsel-assembly-1.0.jar \
load_into_db \
{} \
jdbc:mariadb://172.30.250.53:3308?sessionVariables=sql_mode='ANSI_QUOTES' \
bigdata \
bigdata@123 \
score \
credit_scores_new \
3500 \
subscriber_id,encrypted_score
""".format("{{ next_execution_date.strftime('%Y%m%d') }}",
           score_lead_gen_encrypted_path.format("{{ next_execution_date.strftime('%Y%m%d') }}"))

alter_table_subscribers = """
sudo -u hdfs spark-submit \
--class com.trustingsocial.telkomsel.starter.UtilStart \
--name "alter-table-subscribers-{}" \
--master yarn-cluster \
--driver-cores 2 \
--driver-memory 1g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.instances=1 \
--conf spark.yarn.executor.memoryOverhead=1g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 4 \
--executor-memory 1g \
hdfs://trustiq/apps/libs/ts-telco-telkomsel-assembly-1.0.jar \
alter_table \
jdbc:mariadb://172.30.250.53:3308?sessionVariables=sql_mode='ANSI_QUOTES' \
bigdata \
bigdata@123 \
subscriber_manager \
30000
""".format("{{ next_execution_date.strftime('%Y%m%d') }}")

alter_table_score = """
sudo -u hdfs spark-submit \
--class com.trustingsocial.telkomsel.starter.UtilStart \
--name "alter-table-subscribers-{}" \
--master yarn-cluster \
--driver-cores 2 \
--driver-memory 1g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.instances=1 \
--conf spark.yarn.executor.memoryOverhead=1g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 4 \
--executor-memory 1g \
hdfs://trustiq/apps/libs/ts-telco-telkomsel-assembly-1.0.jar \
alter_table \
jdbc:mariadb://172.30.250.53:3308?sessionVariables=sql_mode='ANSI_QUOTES' \
bigdata \
bigdata@123 \
score \
30000
""".format("{{ next_execution_date.strftime('%Y%m%d') }}")

generate_new_mapping_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--name Generate-NewMsisdn-Mapping-{{ next_execution_date.strftime('%Y%m%d') }} \
--class com.trustingsocial.telco.runners.GenerateNewMapping \
--master yarn-cluster \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.maxExecutors=40 \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--conf \
hdfs://trustiq/apps/etl-commons/msisdn-cols.conf \
--map-table-uri \
hdfs://trustiq/data/mapping/{{ execution_date.strftime('%Y%m%d') }}/all \
-Dvoice="hdfs://trustiq/idea/bi/voice/trx_date\={{ next_execution_date.strftime('%Y%m%d') }}" \
-Dsms="hdfs://trustiq/idea/bi/sms/trx_date\={{ next_execution_date.strftime('%Y%m%d') }}" \
-Ddata_pre="hdfs://trustiq/idea/bi/data_pre/trx_date\={{ next_execution_date.strftime('%Y%m%d') }}" \
-Ddbal="hdfs://trustiq/idea/bi/dbal/trx_date\={{ next_execution_date.strftime('%Y%m%d') }}" \
-Drecharge="hdfs://trustiq/idea/bi/recharge/trx_date\={{ next_execution_date.strftime('%Y%m%d') }}" \
-Dpayment="hdfs://trustiq/idea/bi/payment/trx_date\={{ next_execution_date.strftime('%Y%m%d') }}" \
-Dvas="hdfs://trustiq/idea/bi/vas/trx_date\={{ next_execution_date.strftime('%Y%m%d') }}" \
-Dsubs_info_pre="hdfs://trustiq/idea/bi/subs_info_pre/trx_date\={{ next_execution_date.strftime('%Y%m%d') }}" \
-Dsubs_info_post="hdfs://trustiq/idea/bi/subs_info_post/trx_date\={{ next_execution_date.strftime('%Y%m%d') }}" \
-Ddata_post="hdfs://trustiq/idea/bi/data_post/trx_date\={{ next_execution_date.strftime('%Y%m%d') }}" \
--output-uri \
hdfs://trustiq/data/mapping/{{ next_execution_date.strftime('%Y%m%d') }}/new \
-G \
ds=new_mapping \
--metric-prefix \
idea_mon_ \
--job-name \
cleanup \
--monitoring \
172.30.250.19:9091
"""

count_bi_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--files /etc/spark2/conf/hive-site.xml \
--conf spark.sql.warehouse.dir=hdfs://trustiq/apps/hive/warehouse \
--conf hive.metastore.uris=thrift://iabtrznnd001.ts.local.id:9083 \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=1g \
--executor-cores 4 \
--executor-memory 4g \
--name Count-BI-{dataset}-{date} \
--class com.trustingsocial.applications.CountTotalRecord \
--master yarn-cluster \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--input-path \
hdfs://trustiq/idea/bi/{dataset}/trx_date={date} \
--date \
{date} \
--output-table \
monitoring.total_bi \
--is-dedup \
1 \
--fields \
\* \
--dataset \
{dataset}
"""

merge_mapping_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=2g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 4 \
--executor-memory 6g \
--name MergeMapping-{date} \
--class com.trustingsocial.telco.runners.MergeMappingTables \
--master yarn-cluster \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--map-table1-uri \
hdfs://trustiq/data/mapping/{yesterday}/all \
--map-table2-uri \
hdfs://trustiq/data/mapping/{date}/new \
--output-uri \
hdfs://trustiq/data/mapping/{date}/all \
-G \
ds=merge_mapping \
--metric-prefix \
idea_mon_ \
--monitoring \
172.30.250.19:9091
"""

mapping_for_rocksdb_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.instances=2 \
--conf spark.yarn.executor.memoryOverhead=1g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 2 \
--executor-memory 2g \
--name Mapping-For-RocksDB-{date} \
--class com.trustingsocial.telco.runners.GenerateMappingForRocksDB \
--master yarn-cluster \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--input-path \
hdfs://trustiq/data/mapping/{date}/new \
--output-path \
hdfs://trustiq/data/mapping-csv/{date} \
--app-name \
Mapping-For-RocksDB-{date} \
--partitions \
{partitions} \
{col1} \
{col2}
"""

count_new_mapping_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--files /etc/spark2/conf/hive-site.xml \
--conf spark.sql.warehouse.dir=hdfs://trustiq/apps/hive/warehouse \
--conf hive.metastore.uris=thrift://iabtrznnd001.ts.local.id:9083 \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.executor.instances=10 \
--conf spark.yarn.executor.memoryOverhead=1g \
--executor-cores 4 \
--executor-memory 4g \
--name Count-New-Mapping-{date} \
--class com.trustingsocial.applications.CountTotalRecord \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--input-path \
hdfs://trustiq/data/mapping/{date}/new \
--date \
{date} \
--output-table \
monitoring.total_mapping \
--is-dedup \
0 \
--fields \
\* \
--dataset \
new
"""

count_all_mapping_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--files /etc/spark2/conf/hive-site.xml \
--conf spark.sql.warehouse.dir=hdfs://trustiq/apps/hive/warehouse \
--conf hive.metastore.uris=thrift://iabtrznnd001.ts.local.id:9083 \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.executor.instances=10 \
--conf spark.yarn.executor.memoryOverhead=1g \
--executor-cores 4 \
--executor-memory 4g \
--name Count-All-Mapping-{date} \
--class com.trustingsocial.applications.CountTotalRecord \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--input-path \
hdfs://trustiq/data/mapping/{date}/all \
--date \
{date} \
--output-table \
monitoring.total_mapping \
--is-dedup \
0 \
--fields \
\* \
--dataset \
all
"""

exclude_opt_out_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=1g \
--executor-cores 4 \
--executor-memory 4g \
--name ExcludeOptOut-{dataset}-{date} \
--class com.trustingsocial.telco.runners.Exclude \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--job-name \
ExcludeOptOut-{dataset}-{date} \
--exclude-path \
{opt_out_path} \
--config \
hdfs://trustiq/apps/cleanup/config/{dataset}.conf \
--input-path \
hdfs://trustiq/idea/bi/{dataset}/trx_date={date} \
--output-path \
hdfs://trustiq/data/replace-tmp/{dataset}/trx_date={date} \
--partitions \
{partitions}
"""

replace_msisdn_cid_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=2g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 4 \
--executor-memory 4g \
--name ReplaceMsisdnCid-{dataset}-{date} \
--class com.trustingsocial.telco.runners.ReplaceMsisdnCid \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--conf \
hdfs://trustiq/apps/etl-commons/msisdn-cols.conf \
--map-table-uri \
hdfs://trustiq/data/mapping/{date}/all \
--dataset \
{dataset} \
--dataset-uri \
hdfs://trustiq/data/replace-tmp/{dataset}/trx_date={date} \
--output-uri \
hdfs://trustiq/data/clean-tmp/{dataset}/trx_date={date} \
--enable-counter
"""

cleanup_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=1g \
--executor-cores 4 \
--executor-memory 4g \
--name Cleanup-{dataset}-{date} \
--class com.trustingsocial.telco.runners.RunETL \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--config \
hdfs://trustiq/apps/cleanup/config/{dataset}.conf \
--hash-config \
hdfs://trustiq/hash.conf \
--input-fs \
hdfs://trustiq \
--input-path \
/data/clean-tmp/{dataset}/trx_date={date}/*.parquet \
--output-uri \
/data/clean/{dataset}/trx_date={date} \
--enable-stats \
--auto-optimized-partitions \
--partition-by-col-factor \
20 \
-G \
ds={dataset} \
--metric-prefix \
idea_mon_ \
--job-name \
cleanup \
--monitoring \
172.30.250.19:9091
"""

cleanup_non_msisdn_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--conf spark.executor.instances=10 \
--conf spark.yarn.executor.memoryOverhead=1g \
--executor-cores 4 \
--executor-memory 4g \
--name Cleanup-{dataset}-{date} \
--class com.trustingsocial.telco.runners.RunETL \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--config \
hdfs://trustiq/apps/cleanup-non-msisdn/config/{dataset}.conf \
--hash-config \
hdfs://trustiq/hash.conf \
--input-fs \
hdfs://trustiq \
--input-path \
/idea/bi/{dataset}/trx_date={date}/*.parquet \
--output-uri \
/data/clean/{dataset}/trx_date={date} \
--enable-stats \
--auto-optimized-partitions \
-G \
ds={dataset} \
--metric-prefix \
idea_mon_ \
--job-name \
cleanup \
--monitoring \
172.30.250.19:9091
"""

cleanup_big_datasets_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--conf spark.sql.shuffle.partitions=600 \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=2g \
--executor-cores 4 \
--executor-memory 8g \
--name Cleanup-{dataset}-{date} \
--class com.trustingsocial.telco.runners.RunETL \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--config \
hdfs://trustiq/apps/cleanup/config/{dataset}.conf \
--hash-config \
hdfs://trustiq/hash.conf \
--input-fs \
hdfs://trustiq \
--input-path \
/data/clean-tmp/{dataset}/trx_date={date}/*.parquet \
--output-uri \
/data/clean/{dataset}/trx_date={date} \
--enable-stats \
--auto-optimized-partitions \
--partition-by-col-factor \
20 \
-G \
ds={dataset} \
--metric-prefix \
idea_mon_ \
--job-name \
cleanup \
--monitoring \
172.30.250.19:9091
"""

fs_cleanup_cmd = """
sudo -u hdfs hdfs dfs -rm -r -f hdfs://trustiq/data/replace-tmp/{dataset}/trx_date={date}
sudo -u hdfs hdfs dfs -rm -r -f hdfs://trustiq/data/clean-tmp/{dataset}/trx_date={date}
"""

count_cleanup_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--files /etc/spark2/conf/hive-site.xml \
--conf spark.sql.warehouse.dir=hdfs://trustiq/apps/hive/warehouse \
--conf hive.metastore.uris=thrift://iabtrznnd001.ts.local.id:9083 \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.executor.instances=5 \
--conf spark.yarn.executor.memoryOverhead=1g \
--executor-cores 4 \
--executor-memory 2g \
--name Count-Cleanup-{dataset}-{date} \
--class com.trustingsocial.applications.CountTotalRecord \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--input-path \
/data/clean/{dataset}/trx_date={date} \
--date \
{date} \
--output-table \
monitoring.total_clean \
--is-dedup \
0 \
--fields \
\* \
--dataset \
{dataset}
"""

count_unique_user_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--files /etc/spark2/conf/hive-site.xml \
--conf spark.sql.warehouse.dir=hdfs://trustiq/apps/hive/warehouse \
--conf hive.metastore.uris=thrift://iabtrznnd001.ts.local.id:9083 \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=1g \
--executor-cores 4 \
--executor-memory 4g \
--name Count-Uniq-User-{dataset}-{date} \
--class com.trustingsocial.applications.CountTotalRecord \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--input-path \
/data/clean/{dataset}/trx_date={date} \
--date \
{date} \
--output-table \
monitoring.uniq_user \
--is-dedup \
1 \
--fields \
sid \
--dataset \
{dataset}
"""

archive_cleanup_cmd = """
sudo -u hdfs hdfs dfs -rm -r -f /idea/bi_backup/{dataset}/trx_date={date}
sudo -u hdfs hdfs dfs -mkdir /idea/bi_backup/{dataset}/trx_date={date}
sudo -u hdfs hdfs dfs -mv /idea/bi/{dataset}/trx_date={date}/* /idea/bi_backup/{dataset}/trx_date={date}/
"""

accumulate_subs_info_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.kryoserializer.buffer.max=1g \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=2g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 4 \
--conf spark.sql.shuffle.partitions=600 \
--executor-memory 6g \
--name Accumulate-{dataset}-{date} \
--class com.trustingsocial.telco.runners.RunAccumulator \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--job-name \
Accumulate-{dataset}-{date} \
--input-path \
/data/clean/{dataset}/trx_date={date} \
--pre-input-path \
/data/clean/{dataset}_accumulated/trx_date={yesterday} \
--output-path \
/data/clean/{dataset}_accumulated/trx_date={date} \
--dataset \
{dataset} \
--partitions \
20 \
--partition-by \
{partitionBy} \
--date \
{date}
"""

count_accumulate_subs_info = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--files /etc/spark2/conf/hive-site.xml \
--conf spark.sql.warehouse.dir=hdfs://trustiq/apps/hive/warehouse \
--conf hive.metastore.uris=thrift://iabtrznnd001.ts.local.id:9083 \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.executor.instances=5 \
--conf spark.yarn.executor.memoryOverhead=1g \
--executor-cores 4 \
--executor-memory 2g \
--name Count-Accumulated-{dataset}-{date} \
--class com.trustingsocial.applications.CountTotalRecord \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--input-path \
hdfs://trustiq/data/clean/{dataset}_accumulated/trx_date={date} \
--date \
{date} \
--output-table \
monitoring.total_clean \
--is-dedup \
0 \
--fields \
\* \
--dataset \
{dataset}_accumulated
"""

accumulate_opt_data_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.kryoserializer.buffer.max=1g \
--conf spark.executor.instances=2 \
--conf spark.yarn.executor.memoryOverhead=1g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 2 \
--conf spark.sql.shuffle.partitions=400 \
--executor-memory 2g \
--name Accumulate-{dataset}-{date} \
--class com.trustingsocial.telco.runners.RunAccumulator \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--job-name \
Accumulate-{dataset}-{date} \
--input-path \
hdfs://trustiq/idea/bi/data_opt_in_out/*/trx_date={date} \
--pre-input-path \
hdfs://trustiq/idea/bi/data_opt_in_out_accumulated/{dataset}_accumulated/trx_date={yesterday} \
--output-path \
hdfs://trustiq/idea/bi/data_opt_in_out_accumulated/{dataset}_accumulated/trx_date={date} \
--dataset \
{dataset} \
--partitions \
1 \
--date \
{date}
"""

replace_msisdn_cid_opt_dataset_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.executor.instances=2 \
--conf spark.yarn.executor.memoryOverhead=2g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 4 \
--executor-memory 1g \
--name ReplaceMsisdnCid-{dataset}-{date} \
--class com.trustingsocial.telco.runners.ReplaceMsisdnCid \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--conf \
hdfs://trustiq/apps/etl-commons/msisdn-cols.conf \
--map-table-uri \
hdfs://trustiq/data/mapping/{date}/all \
--dataset \
{dataset} \
--dataset-uri \
hdfs://trustiq/idea/bi/data_opt_in_out_accumulated/{dataset}/trx_date={date} \
--output-uri \
hdfs://trustiq/data/clean-tmp/{dataset}/trx_date={date} \
--enable-counter
"""

cleanup_opt_dataset_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=1g \
--executor-cores 4 \
--executor-memory 4g \
--name Cleanup-{dataset}-{date} \
--class com.trustingsocial.telco.runners.RunETL \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--config \
hdfs://trustiq/apps/cleanup-opt/config/{dataset}.conf \
--hash-config \
hdfs://trustiq/hash.conf \
--input-fs \
hdfs://trustiq \
--input-path \
/data/clean-tmp/{dataset}/trx_date={date}/*.parquet \
--output-uri \
/data/clean/{dataset}/trx_date={date} \
--enable-stats \
--auto-optimized-partitions \
--partition-by-col-factor \
20 \
-G \
ds={dataset} \
--metric-prefix \
idea_mon_ \
--job-name \
cleanup \
--monitoring \
172.30.250.19:9091
"""

replace_msisdn_dnd_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=2g \
--conf spark.storage.memoryFraction=0.1 \
--executor-cores 4 \
--executor-memory 4g \
--name ReplaceMsisdnCid-{dataset}-{date} \
--class com.trustingsocial.telco.runners.ReplaceMsisdnCid \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--conf \
hdfs://trustiq/apps/etl-commons/msisdn-cols.conf \
--map-table-uri \
{mapping_all} \
--dataset \
{dataset} \
--dataset-uri \
/idea/bi/{dataset}/trx_date={date} \
--output-uri \
/data/clean-tmp/{dataset}/trx_date={date} \
--enable-counter
"""

cleanup_dnd_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.executor.instances=20 \
--conf spark.yarn.executor.memoryOverhead=1g \
--executor-cores 4 \
--executor-memory 4g \
--name Cleanup-{dataset}-{date} \
--class com.trustingsocial.telco.runners.RunETL \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--config \
hdfs://trustiq/apps/cleanup/config/{dataset}.conf \
--hash-config \
hdfs://trustiq/hash.conf \
--input-fs \
hdfs://trustiq \
--input-path \
/data/clean-tmp/{dataset}/trx_date={date}/*.parquet \
--output-uri \
/data/clean/{dataset}/trx_date={date} \
--enable-stats \
--auto-optimized-partitions \
--partition-by-col-factor \
20 \
-G \
ds={dataset} \
--metric-prefix \
idea_mon_ \
--job-name \
cleanup \
--monitoring \
172.30.250.19:9091
"""

count_score_subscribers_cmd = """
sudo -u hdfs spark-submit \
--queue default \
--master yarn-cluster \
--files /etc/spark2/conf/hive-site.xml \
--conf spark.sql.warehouse.dir=hdfs://trustiq/apps/hive/warehouse \
--conf hive.metastore.uris=thrift://iabtrznnd001.ts.local.id:9083 \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.executor.instances=5 \
--conf spark.yarn.executor.memoryOverhead=1g \
--executor-cores 4 \
--executor-memory 2g \
--name Count-{dataset}-{date} \
--class com.trustingsocial.applications.CountTotalRecord \
hdfs://trustiq/apps/etl-commons/lib/ETL-assembly-2.1.jar \
--input-path \
/data/score_lead_gen/{dataset}/upto_date={date} \
--date \
{date} \
--output-table \
monitoring.total_clean \
--is-dedup \
0 \
--fields \
\* \
--dataset \
{dataset}
"""
