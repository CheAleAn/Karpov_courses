"""
Отчет по приложению
Соберите единый отчет по работе всего приложения. В отчете должна быть информация и по ленте новостей, и по сервису отправки сообщений.
Продумайте, какие метрики необходимо отобразить в этом отчете? Как можно показать их динамику?  Приложите к отчету графики или файлы, чтобы сделать его более наглядным и информативным. Отчет должен быть не просто набором графиков или текста, а помогать отвечать бизнесу на вопросы о работе всего приложения совокупно.
Автоматизируйте отправку отчета с помощью Airflow. Код для сборки отчета разместите в GitLab.
Отчет должен приходить ежедневно в 11:00 в чат.
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
import numpy as np


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
def chernavin_dag_lesson7_report():

    @task
    def connect_to_bot(token_bot):
        bot = telegram.Bot(token=token_bot)
        return bot

    @task
    def data_by_countries():
        query = """
                SELECT
                toDate(time) as Date,
                country as Country,
                count(distinct user_id) as "Users per country",
                'Feed' as table
                FROM simulator_20221020.feed_actions
                where toDate(time) >= today()-30
                group by toDate(time), country
                Union all
                SELECT
                toDate(time) as Date,
                country as Country,
                count(distinct user_id) as "Users per country",
                'Message' as table
                FROM simulator_20221020.message_actions
                where toDate(time) >= today()-30
                group by toDate(time), country
        """
        return pandahouse.read_clickhouse(query, connection=connection_to_work_db)

    @task
    def avg_usage_feed_app():
        query = """
            select
            Date,
            avg(Views) as "Average views by user",
            avg(Likes) as "Average likes by user"
            from (
                select
                toDate(time) as Date,
                user_id,
                countIf(action='view') as Views,
                countIf(action='like') as Likes
                FROM simulator_20221020.feed_actions
                where toDate(time) >= today()-30
                group by toDate(time), user_id
            )
            where Views>=10
            group by Date
        """

        return pandahouse.read_clickhouse(query, connection=connection_to_work_db)

    @task
    def avg_usage_mes_app():
        query = """
            select
            Date,
            avg(sent_mes) as Sent_messages
            from (
                SELECT
                toDate(time) as Date,
                user_id,
                count(user_id) as sent_mes
                from simulator_20221020.message_actions
                WHERE toDate(time) >= today() - 30
                group by toDate(time), user_id
            )
            where sent_mes>5
            group by Date
        """
        return pandahouse.read_clickhouse(query, connection=connection_to_work_db)

    @task
    def source_feed_app():
        query = """
            select
            toDate(time) as Date,
            source as Source,
            count(distinct user_id) as "Active users"
            from simulator_20221020.feed_actions
            where toDate(time) >= today()-30
            group by toDate(time), source
        """
        return pandahouse.read_clickhouse(query, connection=connection_to_work_db)

    @task
    def source_mes_app():
        query = """
            select
            toDate(time) as Date,
            source as Source,
            count(distinct user_id) as "Active users"
            from simulator_20221020.message_actions
            where toDate(time) >= today()-30
            group by toDate(time), source
        """
        return pandahouse.read_clickhouse(query, connection=connection_to_work_db)

    @task
    def feed_dynamic_by_week():
        query = """
            select
                weeks.week as Week,
                active.active_users - new.new_users as Retained,
                new.new_users as New,
                active.active_users - new.new_users - prev_active.active_users as Gone,
                if(prev_active.active_users>0, (active.active_users - new.new_users) / prev_active.active_users * 100, 0) as Stayed
            from (
                select
                    DISTINCT toStartOfWeek(time) as week
                from simulator_20221020.feed_actions ) weeks
            left join (
                select
                    toStartOfWeek(time) as week,
                    count(DISTINCT user_id) active_users
                from simulator_20221020.feed_actions
                group by toStartOfWeek(time)
            ) active
            on weeks.week=active.week
            left join (
                select
                    first_time as week,
                    count(user_id) new_users
                from (
                    select user_id,
                    min(toStartOfWeek(time)) as first_time
                    from simulator_20221020.feed_actions
                    group by user_id
                )
                group by first_time
            ) new
            on weeks.week=new.week
            left join (
                select
                    toStartOfWeek(time) as week,
                    count(DISTINCT user_id) active_users
                from simulator_20221020.feed_actions
                group by toStartOfWeek(time)
            ) prev_active
            on weeks.week=prev_active.week+7
            order by weeks.week
        """
        return pandahouse.read_clickhouse(query, connection=connection_to_work_db)

    @task
    def mes_dynamic_by_week():
        query = """
            select
                weeks.week as Week,
                active.active_users - new.new_users as Retained,
                new.new_users as New,
                active.active_users - new.new_users - prev_active.active_users as Gone,
                if(prev_active.active_users>0, (active.active_users - new.new_users) / prev_active.active_users * 100, 0) as Stayed
            from (
                select
                    DISTINCT toStartOfWeek(time) as week
                from simulator_20221020.message_actions ) weeks
            left join (
                select
                    toStartOfWeek(time) as week,
                    count(DISTINCT user_id) active_users
                from simulator_20221020.message_actions
                group by toStartOfWeek(time)
            ) active
            on weeks.week=active.week
            left join (
                select
                    first_time as week,
                    count(user_id) new_users
                from (
                    select user_id,
                    min(toStartOfWeek(time)) as first_time
                    from simulator_20221020.message_actions
                    group by user_id
                )
                group by first_time
            ) new
            on weeks.week=new.week
            left join (
                select
                    toStartOfWeek(time) as week,
                    count(DISTINCT user_id) active_users
                from simulator_20221020.message_actions
                group by toStartOfWeek(time)
            ) prev_active
            on weeks.week=prev_active.week+7
            order by weeks.week
        """
        return pandahouse.read_clickhouse(query, connection=connection_to_work_db)

    @task
    def service_usage():
        query = """
            select
            m.date as Date,
            m.message_only_users+f.feed_only_users+b.both_service_users as "All users",
            m.message_only_users/(m.message_only_users+f.feed_only_users+b.both_service_users) as "Message only users",
            f.feed_only_users/(m.message_only_users+f.feed_only_users+b.both_service_users) as "Feed only users",
            b.both_service_users/(m.message_only_users+f.feed_only_users+b.both_service_users) as "Both service users"
            from (
              select
              date,
              count(*) as message_only_users
              FROM (
                SELECT
                distinct user_id, toDate(time) as date
                FROM simulator_20221020.message_actions
                except
                select distinct user_id, toDate(time) as date
                FROM simulator_20221020.feed_actions
              )
              group by date
            ) m
            join (
              select
              date,
              count(*) as feed_only_users
              FROM (
                SELECT
                distinct user_id, toDate(time) as date
                FROM simulator_20221020.feed_actions
                except
                select distinct user_id, toDate(time) as date
                FROM simulator_20221020.message_actions
              )
              group by date
            ) f
            on f.date=m.date
            join (
              select
              date,
              count(*) as both_service_users
              from (
                SELECT
                distinct user_id, toDate(time) as date
                FROM simulator_20221020.feed_actions
              ) f
              join
              (
                SELECT
                distinct user_id, toDate(time) as date
                FROM simulator_20221020.message_actions
              ) m
              on f.date=m.date and f.user_id=m.user_id
              group by date
            ) b
            on b.date=f.date
        """
        return pandahouse.read_clickhouse(query, connection=connection_to_work_db)

    @task
    def plotting_by_countries(data, bot, chat_id):
        data = data[(data['Users per country']>=250) & (data['table']=='Feed')]
        plt.figure(figsize=(15,8))
        g = sns.lineplot(data=data, x='Date', y='Users per country', hue='Country')
        g.set(ylabel=None)
        plt.grid()
        plt.xticks(rotation=45)
        plt.title('Users by countries')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, disable_notification=True)

    @task
    def plotting_avg_usage_feed(data, bot, chat_id):
        plt.figure(figsize=(15,8))
        g = plt.plot(data['Date'], data['Average views by user'], 'r--',\
                    data['Date'], data['Average likes by user'],'b-')
        plt.legend(['Average views by user', 'Average likes by user'])
        plt.ylim(bottom=0)
        plt.grid()
        plt.xticks(rotation=45)
        plt.title('Usage of feed app')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, disable_notification=True)

    @task
    def plotting_avg_usage_mes(data, bot, chat_id):
        plt.figure(figsize=(15,8))
        g = plt.plot(data['Date'], data['Sent_messages'], 'r--')
        plt.legend(['Sent_messages'])
        plt.ylim(bottom=0)
        plt.grid()
        plt.xticks(rotation=45)
        plt.title('Usage of message app')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, disable_notification=True)

    @task
    def plotting_source_feed(data, bot, chat_id):
        plt.figure(figsize=(15,8))
        g = sns.lineplot(data=data, x='Date', y='Active users', marker="o", hue='Source')
        plt.ylim(bottom=0)
        plt.grid()
        plt.xticks(rotation=45)
        plt.title('Feed app source')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, disable_notification=True)

    @task
    def plotting_source_mes(data, bot, chat_id):
        plt.figure(figsize=(15,8))
        g = sns.lineplot(data=data, x='Date', y='Active users', marker="o", hue='Source')
        plt.ylim(bottom=0)
        plt.grid()
        plt.xticks(rotation=45)
        plt.title('Message app source')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, disable_notification=True)

    @task
    def plotting_week_stayed_users(data_feed, data_mes, bot, chat_id):
        plt.figure(figsize=(15,8))
        g = plt.plot(data_feed['Week'], data_feed['Stayed'], 'r--',\
                    data_mes['Week'], data_mes['Stayed'],'b-')
        plt.legend(['Feed', 'Message'])
        plt.ylim(bottom=0)
        plt.grid()
        plt.xticks(rotation=45)
        plt.title('Stayed users from previous week')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, disable_notification=True)

    @task
    def plotting_dynamic_feed_users(data, bot, chat_id):
        plt.figure(figsize=(15,8))
        index = np.arange(0,data.shape[0])
        bar_width = 0.3
        g = plt.bar(index-bar_width, data['New'], bar_width, color='b')
        g = plt.bar(index, data['Retained'], bar_width, color='g')
        g = plt.bar(index+bar_width, data['Gone'], bar_width, color='r')
        plt.legend(['New', 'Retained', 'Gone'])
        plt.grid()
        plt.xticks(rotation=90)
        plt.title('Feed app users dynamic')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plt.xticks(index, data['Week'].dt.date)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, disable_notification=True)

    @task
    def plotting_dynamic_mes_users(data, bot, chat_id):
        plt.figure(figsize=(15,8))
        index = np.arange(0,data.shape[0])
        bar_width = 0.3
        g = plt.bar(index-bar_width, data['New'], bar_width, color='b')
        g = plt.bar(index, data['Retained'], bar_width, color='g')
        g = plt.bar(index+bar_width, data['Gone'], bar_width, color='r')
        plt.legend(['New', 'Retained', 'Gone'])
        plt.grid()
        plt.xticks(rotation=90)
        plt.title('Message app users dynamic')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plt.xticks(index, data['Week'].dt.date)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, disable_notification=True)

    @task
    def plotting_services_usages(data, bot, chat_id):
        plt.figure(figsize=(15,8))
        index = np.arange(0, data.shape[0])
        bar_width = 0.5
        g = plt.bar(index, data['Feed only users'],bar_width, edgecolor='white', color='b')
        g = plt.bar(index, data['Both service users'],bar_width,bottom=data['Feed only users'], color='g')
        g = plt.bar(index, data['Message only users'],bar_width,\
                    bottom=[(i+j) for i,j in zip(data['Feed only users'], data['Both service users'])],\
                    color='r')
        plt.legend(['Only Feed', 'Both', 'Only Message'])
        plt.grid()
        plt.xticks(rotation=90)
        plt.title('Apps usage for last month')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plt.xticks(index, data['Date'].dt.date)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object, disable_notification=True)

    #bot = connect_to_bot(token_bot)
    data_by_countries = data_by_countries()
    plotting_by_countries(data_by_countries, bot, chat_id)
    avg_usage_feed_app = avg_usage_feed_app()
    plotting_avg_usage_feed(avg_usage_feed_app, bot, chat_id)
    avg_usage_mes_app = avg_usage_mes_app()
    plotting_avg_usage_mes(avg_usage_mes_app, bot, chat_id)
    source_feed_app = source_feed_app()
    plotting_source_feed(source_feed_app, bot, chat_id)
    source_mes_app = source_mes_app()
    plotting_source_mes(source_mes_app, bot, chat_id)
    feed_dynamic_by_week = feed_dynamic_by_week()
    plotting_dynamic_feed_users(feed_dynamic_by_week, bot, chat_id)
    mes_dynamic_by_week = mes_dynamic_by_week()
    plotting_dynamic_mes_users(mes_dynamic_by_week, bot, chat_id)
    plotting_week_stayed_users(feed_dynamic_by_week, mes_dynamic_by_week, bot, chat_id)
    service_usage = service_usage()
    plotting_services_usages(service_usage, bot, chat_id)

chernavin_dag_lesson7_report = chernavin_dag_lesson7_report()
