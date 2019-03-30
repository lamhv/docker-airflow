spark_pool = 'spark-pool-4'
check_output_pool = 'check-output-pool'
check_bi_pool = 'check-bi-pool'
cleanup_pool = 'cleanup-pool'
check_clean_pool = 'check-clean-pool'

accepted_path = '/data/score_lead_gen/score_accepted/upto_date={}'
reference_path = "{{ task_instance.xcom_pull(task_ids='get-latest-score-accepted') }}"
active_score_path = '/data/working/prod_v00/creditscore/score_v01_01_active/upto_date={}'
score_lead_gen_encrypted_path = "/data/score_lead_gen/score_encrypted/upto_date={}"
location_path = '/data/working/prod_v00/idata/home-pin-8wkagg/upto_date={}'
merged_location_path = '/data/score_lead_gen/merged_location/upto_date={}'
tags_path = '/data/working/leadgen_team/sub_tags/combine_tag/upto_date={}'
subscribers_path = '/data/score_lead_gen/subscribers/upto_date={}'

local_path = '/home/production/ds-code-airflow'
data_working_path = '/data/working/prod_v00'
