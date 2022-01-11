from google.cloud import bigquery
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import requests
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from cre import *

token_failed=cre_token_failed
mysql_connection="poc_mysql_gcp"
client=bigquery.Client(project='cbd-bi-dev')

def check_coupon_missing(**kwargs):
    try:
        print('start')
        try:
            days=7 if kwargs['dag_run'].conf.get('days')==None else kwargs['dag_run'].conf.get('days')
        except Exception as days_error:
            days=7
        print(days)
        coupon_name_today=[]
        coupon_name_default=['Coupon Carabao Dang','Coupon Cash 100']

        query_stm="""SELECT CouponName FROM `cbd-bi-dev.cj.dp_cj_redeem` 
        WHERE Partition_ImportDate = date_sub(current_date('Asia/Bangkok'),interval {} day)
        group by CouponName
        LIMIT 1000""".format(days)

        query_job = client.query(query_stm)  # Make an API request.
        rows = query_job.result()

        for i in rows:
            coupon_name_today.append(i.CouponName)

        coupon_missing=list(set(coupon_name_default)-set(coupon_name_today))
        if len(coupon_missing)==0:
            print("Coupon checked")
            kwargs['ti'].xcom_push(key='days',value=days)
            return 'check_rowdup_task'
        else:
            coupon_missing_name=''
            for c in coupon_missing:
                coupon_missing_name+=c+' '
            kwargs['ti'].xcom_push(key='coupon_missing_name',value=coupon_missing_name)
            return 'noti_task'
    except Exception as e1:
        print(e1)     


def check_rowdup(**kwargs):
    days=int(kwargs['ti'].xcom_pull(task_ids='check_coupon_missing_task',key='days'))
    print('start')
    query_stm="""SELECT RowId,count(*) as cnt
    FROM `cbd-bi-dev.cj.dp_cj_redeem` 
    WHERE Partition_ImportDate = date_sub(current_date('Asia/Bangkok'),interval {} day)
    group by RowId
    having cnt>1""".format(days)
    query_job=client.query(query_stm)
    rows=query_job.result().total_rows
    print(rows)
    if rows==0:
        kwargs['ti'].xcom_push(key='message_rowdup',value='No RowId duplicate')
        return 'insert_db_task'
    else:
        kwargs['ti'].xcom_push(key='message_rowdup',value='RowId duplicate')
        return 'noti_task'


def insert_db(**kwargs):
    days=7 if kwargs['dag_run'].conf.get('days')==None else kwargs['dag_run'].conf.get('days')
    print('start')
    query_stm="""SELECT *
    FROM `cbd-bi-dev.cj.dp_cj_redeem`
    WHERE Partition_ImportDate = date_sub(current_date('Asia/Bangkok'),interval {} day)""".format(days)
    query_job=client.query(query_stm)
    df=query_job.result().to_dataframe()
    print('start insert')
    
    try:
        df=df.values.tolist()
        mysql=MySqlHook(mysql_connection)
        mysql.insert_rows(table='cj_redeem',rows=df)
        kwargs['ti'].xcom_push(key='message',value='succeeded')
    except Exception as e1:
        print(e1)
        kwargs['ti'].xcom_push(key='message',value='failed')

def noti(**kwargs):
    try:
        message="cj redeem coupon "+kwargs['ti'].xcom_pull(task_ids='check_coupon_missing_task',key='coupon_missing_name')+'missing'
    except Exception as e1:
        try:
            message="cj redeem coupon "+kwargs['ti'].xcom_pull(task_ids='check_rowdup_task',key='message_rowdup')
        except Exception as e2:
            message=kwargs['message']

    url_sticker = '&stickerPackageId=6632&stickerId=11825375'
    url = 'https://notify-api.line.me/api/notify?message={}{}'.format(message,url_sticker)
    headers={'Content-Type':'application/x-www-form-urlencoded','Authorization': 'Bearer '+token_failed}
    print("sending noti...")
    resp = requests.post(url,headers=headers)

def noti_insert(**kwargs):
    try:
        message="cj redeem coupon "+kwargs['ti'].xcom_pull(task_ids='insert_db_task',key='message')
    except Exception as e1:
        message="failed message"

    url_sticker = '&stickerPackageId=6632&stickerId=11825375'
    url = 'https://notify-api.line.me/api/notify?message={}{}'.format(message,url_sticker)
    headers={'Content-Type':'application/x-www-form-urlencoded','Authorization': 'Bearer '+token_failed}
    print("sending noti...")
    resp = requests.post(url,headers=headers)

dag=DAG(
    "cj_redeem",
    start_date=days_ago(1),
    schedule_interval=None
)

check_coupon_missing_task = BranchPythonOperator(
    task_id='check_coupon_missing_task',
    python_callable=check_coupon_missing,
    provide_context=True,
    dag=dag
    )

noti_task = PythonOperator(
    task_id="noti_task",
    python_callable=noti,
    op_kwargs={'message':'no coupon missing'},
    dag=dag
    )
    
noti_rowdup_task = PythonOperator(
    task_id="noti_rowdup_task",
    python_callable=noti,
    provide_context=True,
    dag=dag
)

check_rowdup_task = BranchPythonOperator(
    task_id="check_rowdup_task",
    python_callable=check_rowdup,
     provide_context=True,
    dag=dag
)

insert_db_task= PythonOperator(
    task_id="insert_db_task",
    python_callable=insert_db,
    provide_context=True,
    dag=dag
)

noti_insert_task = PythonOperator(
    task_id="noti_insert_task",
    python_callable=noti_insert,
    provide_context=True,
    dag=dag
)
 
check_coupon_missing_task >> check_rowdup_task >> noti_rowdup_task 
check_coupon_missing_task >> check_rowdup_task >> insert_db_task >> noti_insert_task
check_coupon_missing_task >> noti_task
