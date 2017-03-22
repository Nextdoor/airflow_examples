import logging
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook


def get_num_active_dagruns(dag_id, conn_id='airflow_db'):
    airflow_db = PostgresHook(postgres_conn_id=conn_id)
    conn = airflow_db.get_conn()
    cursor = conn.cursor()
    sql = """
select count(*)
from public.dag_run
where dag_id = '{dag_id}'
  and state in ('running', 'queued', 'up_for_retry')
""".format(dag_id=dag_id)
    cursor.execute(sql)
    num_active_dagruns = cursor.fetchone()[0]
    return num_active_dagruns

def is_latest_active_dagrun(**kwargs):
    """Ensure that there are no runs currently in progress and this is the most recent run."""
    num_active_dagruns = get_num_active_dagruns(kwargs['dag'].dag_id)
    logging.info(num_active_dagruns)
    # first, truncate the date to the schedule_interval granularity, then subtract the schedule_interval
    schedule_interval = kwargs['dag'].schedule_interval
    now_epoch = int(datetime.now().strftime('%s'))
    now_epoch_truncated = now_epoch - (now_epoch % schedule_interval.total_seconds())
    expected_run_epoch = now_epoch_truncated - schedule_interval.total_seconds()
    expected_run_execution_date = datetime.fromtimestamp(expected_run_epoch)
    is_latest_dagrun = kwargs['execution_date'] == expected_run_execution_date
    logging.info("Is latest dagrun: " + str(is_latest_dagrun))
    logging.info("Num dag runs active: " + str(num_active_dagruns))
    # If return value is False, then all downstream tasks will be skipped
    is_latest_active_dagrun = (is_latest_dagrun and num_active_dagruns == 1)
    return is_latest_active_dagrun
