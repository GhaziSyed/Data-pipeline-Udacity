from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements

# Properties of DAG object
default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now()
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# Instantiate the DAG object using Airflow-1 code style
@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
# Instantiate the pipline function
def final_project():

    '''Create the Airflow tasks for automated ETL of Sparkify data'''

    start = DummyOperator(task_id='Begin_Pipeline')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag,
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="log_data",
        region="us-east-1",
        extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag=dag,
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="song_data/A/A/A",
        region="us-east-1",
        extra_params="JSON 'auto' COMPUPDATE OFF"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        table='songplays',
        redshift_conn_id="redshift",
        load_sql_stmt=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        table='users',
        redshift_conn_id="redshift",
        truncate_table=True,
        load_sql_stmt=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        table='songs',
        redshift_conn_id="redshift",
        truncate_table=True,
        load_sql_stmt=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        table='artists',
        redshift_conn_id="redshift",
        truncate_table=True,
        load_sql_stmt=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        table='time',
        redshift_conn_id="redshift",
        truncate_table=True,
        load_sql_stmt=SqlQueries.time_table_insert
    )

    quality_checks = DataQualityOperator(
    task_id='data_quality_checks',
    dag=dag,
    dq_checks=[
        { 'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'expected_result': 2 }
    ],
    redshift_conn_id="redshift"
    )

    end = DummyOperator(task_id='End_Pipeline',  dag=dag)

    '''Order the execution of the tasks'''

    # Start Airflow Pipeline 
    start >> stage_events_to_redshift
    start >> stage_songs_to_redshift
    # Create Staging Tables
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    # Create Destination Tables
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
    # Perform Data Quality Check
    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks
    # End Airflow Pipeline 
    quality_checks >> end

final_project_dag = final_project()
