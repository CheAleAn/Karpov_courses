"""
ПОСТРОЕНИЕ ETL-ПАЙПЛАЙНА
1. Параллельно будем обрабатывать две таблицы. В feed_actions для каждого юзера посчитаем число просмотров и лайков контента. В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. Каждая выгрузка должна быть в отдельном таске.
2. Далее объединяем две таблицы в одну.
3. Для этой таблицы считаем все эти метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез.
4. И финальные данные со всеми метриками записываем в отдельную таблицу в ClickHouse.
5. Каждый день таблица должна дополняться новыми данными.
Структура финальной таблицы должна быть такая:
Дата - event_date
Название среза - dimension
Значение среза - dimension_value
Число просмотров - views
Числой лайков - likes
Число полученных сообщений - messages_received
Число отправленных сообщений - messages_sent
От скольких пользователей получили сообщения - users_received
Скольким пользователям отправили сообщение - users_sent
Срез - это os, gender и age
Вашу таблицу необходимо загрузить в схему test, ответ на это задание - название Вашей таблицы в схеме test
"""

import pandas as pd
import pandahouse
import requests
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

metrics_order = ['event_date','dimension','dimension_value',\
                                 'views', 'likes', \
                                 'messages_received', 'messages_sent',\
                                 'users_received', 'users_sent']

connection_to_work_db = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20221020',
    'user': 'student',
    'password': 'dpo_python_2020'
}

connection_to_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'test',
    'user': 'student-rw',
    'password': '656e2b0c9c'
}

default_args = {
'owner': 'a-chernavin-12',
'depends_on_past': False,
'retries': 3,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2022, 11, 13),
}

schedule_interval = '0 10 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def chernavin_dag_lesson6_upload_task():

    @task
    def extract_feed_activity():
        feed_query = """
            SELECT
            user_id, gender, age, os, toDate(time) as event_date,
            sum(action='view') as views,
            sum(action='like') as likes
            FROM simulator_20221020.feed_actions
            WHERE toDate(time) = yesterday()
            group by user_id, gender, age, os, toDate(time)
        """
        return pandahouse.read_clickhouse(feed_query, connection=connection_to_work_db)

    @task
    def extract_messages_activity():
        messages_query = """
            select *
            from (
                SELECT
                    user_id, gender, age, os, toDate(time) as event_date,
                    count(reciever_id) as messages_sent,
                    count(distinct reciever_id) as users_sent
                FROM simulator_20221020.message_actions
                WHERE toDate(time) = yesterday()
                group by user_id, gender, age, os, toDate(time)
            ) sent
            join (
                select reciever_id,
                    count(user_id) as messages_received,
                    count(distinct user_id) as users_received
                FROM simulator_20221020.message_actions
                WHERE toDate(time) = yesterday()
                group by reciever_id
            ) recieved
            on sent.user_id=recieved.reciever_id
        """
        return pandahouse.read_clickhouse(messages_query, connection=connection_to_work_db)

    @task
    def concat_origin_tables(feed_table, mes_table):
        table = pd.merge(feed_table, mes_table, on=['user_id', 'gender', 'os', 'age', 'event_date'], how='outer').fillna(0)
        table[['views', 'likes',
       'messages_sent', 'users_sent', 'reciever_id', 'messages_received',
       'users_received']] = table[['views', 'likes',
       'messages_sent', 'users_sent', 'reciever_id', 'messages_received',
       'users_received']].astype('uint64')
        return table

    @task
    def gender_dimension(table):
        gender_table = table.groupby(['event_date','gender'])\
        ['views', 'likes', 'messages_sent', 'users_sent', 'messages_received', 'users_received']\
        .sum().reset_index().rename(columns={'gender':'dimension_value'})
        gender_table['dimension'] = 'gender'
        gender_table['dimension_value'] = gender_table['dimension_value'].replace({0:'Female', 1:'Male'})
        return gender_table[metrics_order]

    @task
    def os_dimension(table):
        os_table = table.groupby(['event_date','os'])\
            ['views', 'likes', 'messages_sent', 'users_sent', 'messages_received', 'users_received']\
            .sum().reset_index().rename(columns={'os':'dimension_value'})
        os_table['dimension'] = 'os'
        return os_table[metrics_order]

    @task
    def age_dimension(table):
        age_table = table.groupby(['event_date','age'])\
            ['views', 'likes', 'messages_sent', 'users_sent', 'messages_received', 'users_received']\
            .sum().reset_index().rename(columns={'age':'dimension_value'})
        age_table['dimension'] = 'age'
        return age_table[metrics_order]

    @task
    def concat_final_tables(gender_table,os_table,age_table):
        return pd.concat([gender_table,os_table,age_table]).reset_index(drop=True)

    @task
    def upload_table(table):
        query = '''
        CREATE TABLE IF NOT EXISTS test.chernavin_lesson6_task
        (
        event_date Date,
        dimension String,
        dimension_value String,
        views Int64,
        likes Int64,
        messages_received Int64,
        messages_sent Int64,
        users_received Int64,
        users_sent Int64
        )
        ENGINE = MergeTree()
        ORDER BY event_date
        '''
        pandahouse.execute(query, connection=connection_to_test)
        pandahouse.to_clickhouse(table, 'chernavin_lesson6_task',
                         connection=connection_to_test, index=False)


    feed_table = extract_feed_activity()
    mes_table = extract_messages_activity()
    origin_table = concat_origin_tables(feed_table, mes_table)
    gender_table = gender_dimension(origin_table)
    os_table = os_dimension(origin_table)
    age_table = age_dimension(origin_table)
    final_table = concat_final_tables(gender_table,os_table,age_table)
    upload_table(final_table)

chernavin_dag_lesson6_upload_task = chernavin_dag_lesson6_upload_task()
