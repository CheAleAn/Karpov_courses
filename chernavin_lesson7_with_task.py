"""
Отчет по ленте
Итак, пришло время автоматизировать базовую отчетность нашего приложения.  Давайте наладим автоматическую отправку аналитической сводки в телеграм каждое утро!
Напишите скрипт для сборки отчета по ленте новостей. Отчет должен состоять из двух частей:
текст с информацией о значениях ключевых метрик за предыдущий день
график с значениями метрик за предыдущие 7 дней
Отобразите в отчете следующие ключевые метрики:
DAU
Просмотры
Лайки
CTR
Автоматизируйте отправку отчета с помощью Airflow. Код для сборки отчета разместите в GitLab
"""

import telegram
import pandahouse
import matplotlib.pyplot as plt
import io
import seaborn as sns
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import pandas as pd
from datetime import datetime, timedelta


connection_to_work_db = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20221020',
    'user': 'student',
    'password': 'dpo_python_2020'
}

#не подсматривайте! :)
chat_id = 280164205
token_bot='5771887678:AAEipb4Zes3txI5o_nuJtioH37g7vbkkw30'


bot = telegram.Bot(token=token_bot)
bot = telegram.Bot(token=token_bot)
#не знаю почему, но создание объекта Бот Телеграма отдельной функцией
#не работает. Более того, с первого раза к боту телеграма
#подключение у меня не получается (ни с сервера, ни с домашнего пк).
#пока костыль

default_args = {
'owner': 'a-chernavin-12',
'depends_on_past': False,
'retries': 3,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2022, 11, 13),
}

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def chernavin_dag_lesson7():

    @task
    def connect_to_bot(token_bot):
        bot = telegram.Bot(token=token_bot)
        return bot

    @task
    def getting_data():
        query = """
                select
                toDate(time) as Day,
                count(distinct user_id) as DAU,
                countIf(action='view') as Views,
                countIf(action='like') as Likes,
                countIf(action='like')/countIf(action='view')*100 as CTR
                from simulator_20221020.feed_actions
                where toDate(time) >= today() - 7 and toDate(time) <= yesterday()
                group by toDate(time)
                order by Day
        """
        return pandahouse.read_clickhouse(query, connection=connection_to_work_db)

    @task
    def text_message(data, bot, chat_id):
        msg = f"""Cтатистика за вчера:
        DAU = {data.iloc[-1,1]} users
        Views = {data.iloc[-1,2]}
        Likes = {data.iloc[-1,3]}
        CTR = {data.iloc[-1,4]:.2f}%"""
        bot.sendMessage(chat_id=chat_id, text=msg)

    @task
    def plotting_DAU(data, bot, chat_id):
        plt.figure(figsize=(15,8))
        g = sns.lineplot(data=data, x='Day', y='DAU', marker="o")
        g.set(ylabel=None)
        plt.grid()
        plt.xticks(rotation=45)
        plt.title('DAU')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, disable_notification=True)

    @task
    def plotting_Views(data, bot, chat_id):
        plt.figure(figsize=(15,8))
        g = sns.lineplot(data=data, x='Day', y='Views', marker="o")
        g.set(ylabel=None)
        plt.grid()
        plt.xticks(rotation=45)
        plt.title('Views')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, disable_notification=True)

    @task
    def plotting_Likes(data, bot, chat_id):
        plt.figure(figsize=(15,8))
        g = sns.lineplot(data=data, x='Day', y='Likes', marker="o")
        g.set(ylabel=None)
        plt.grid()
        plt.xticks(rotation=45)
        plt.title('Likes')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, disable_notification=True)

    @task
    def plotting_CTR(data, bot, chat_id):
        plt.figure(figsize=(15,8))
        g = sns.lineplot(data=data, x='Day', y='CTR', marker="o")
        g.set(ylabel=None)
        plt.grid()
        plt.xticks(rotation=45)
        plt.title('CTR')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, disable_notification=True)

    #bot = connect_to_bot(token_bot)
    week_data = getting_data()
    text_message(week_data, bot, chat_id)
    plotting_DAU(week_data, bot, chat_id)
    plotting_Views(week_data, bot, chat_id)
    plotting_Likes(week_data, bot, chat_id)
    plotting_CTR(week_data, bot, chat_id)

chernavin_dag_lesson7 = chernavin_dag_lesson7()
