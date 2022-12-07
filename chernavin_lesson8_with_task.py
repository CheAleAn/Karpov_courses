'''
Напишите систему алертов для нашего приложения
Система должна с периодичность каждые 15 минут проверять ключевые метрики, такие как активные пользователи в ленте / мессенджере, просмотры, лайки, CTR, количество отправленных сообщений.
Изучите поведение метрик и подберите наиболее подходящий метод для детектирования аномалий. На практике как правило применяются статистические методы.
В случае обнаружения аномального значения, в чат должен отправиться алерт - сообщение со следующей информацией: метрика, ее значение, величина отклонения.
В сообщение можно добавить дополнительную информацию, которая поможет при исследовании причин возникновения аномалии, это может быть, например,  график, ссылки на дашборд/чарт в BI системе.
'''



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
chat_id = -715927362 #karpov courses alert
#chat_id = 280164205 #мой диалог с ботом
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
'retry_delay': timedelta(minutes=2),
'start_date': datetime(2022, 11, 20),
}

schedule_interval = '00,15,30,45 * * * *'

metrics = ['Views', 'Likes', 'CTR', 'Feed_users', 'Mes_users', 'Sent_messages']


def text_msg(df, metric):
    msg = f"""Метрика {metric}.
Текущее значение {df[metric].iloc[-2]:.2f}. Отклонение более {abs(1-df[metric].iloc[-2]/df[metric].iloc[-3]):.2%}.
"""
    bot.sendMessage(chat_id=chat_id, text=msg)

def print_problem_metric(df, metric):
    text_msg(df, metric)
    df_to_plot = df.iloc[-96:].copy().reset_index(drop=True)

    plt.figure(figsize=(25,25))
    plt.tight_layout()
    ax = sns.lineplot(x=df_to_plot['HM'], y=df_to_plot[metric], label='metric')
    ax = sns.lineplot(x=df_to_plot['HM'], y=df_to_plot['up'], label='up')
    ax = sns.lineplot(x=df_to_plot['HM'], y=df_to_plot['low'], label='low')

    for ind, label in enumerate(ax.get_xticklabels()):
        if ind % 4 == 0:
            label.set_visible(True)
        else:
            label.set_visible(False)

    plt.xticks(rotation=45)
    plt.title(metric.capitalize())
    plt.grid()
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    bot.sendPhoto(chat_id=chat_id, photo=plot_object, disable_notification=True)

def checking_by_history(df,metric,current_period,a=2):
    periods = list()
    for j in range(-3,4,1):
        periods.append(str((current_period+timedelta(minutes=15*j)).time().strftime('%H:%M')))

    df_check = df[df['HM'].isin(periods)][['period',metric]].copy().reset_index(drop=True)
    df_check['q25'] = df_check[metric].shift(1).rolling(df_check.shape[0]-1).quantile(0.25)
    df_check['q75'] = df_check[metric].shift(1).rolling(df_check.shape[0]-1).quantile(0.75)
    df_check['iqr'] = df_check['q75'] - df_check['q25']
    df_check['low'] = df_check['q25'] - a*df_check['iqr']
    df_check['up'] = df_check['q75'] + a*df_check['iqr']
    #print(df_check.iloc[-1])
    return df_check.iloc[-1].to_dict()

def rolling_df(df):
    df['low'] = df['low'].rolling(5, center=True, min_periods=1).mean()
    df['up'] = df['up'].rolling(5, center=True, min_periods=1).mean()
    return df


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def chernavin_dag_lesson8():

    @task
    def connect_to_bot(token_bot):
        bot = telegram.Bot(token=token_bot)
        return bot

    @task
    def getting_data():
        query = """
            select
            mes.date,
            mes.period,
            formatDateTime(mes.period, '%R') as HM,
            mes.Mes_users,
            mes.Sent_messages,
            feed.Views,
            feed.Likes,
            feed.CTR,
            feed.Feed_users
            from (
                SELECT
                toDate(time) as date,
                toStartOfFifteenMinutes(time) as period,
                count(distinct user_id) as Mes_users,
                count(user_id) as Sent_messages
                from simulator_20221020.message_actions
                WHERE or(toDate(time) >= today() - 1,
                    toDate(time) = today() - 7,
                    toDate(time) = today() - 8,
                    toDate(time) = today() - 14,
                    toDate(time) = today() - 15)
                group by toDate(time), toStartOfFifteenMinutes(time) as period
            ) mes
            join (
                SELECT
                toDate(time) as date,
                toStartOfFifteenMinutes(time) as period,
                sum(action='view') as Views,
                sum(action='like') as Likes,
                sum(action='like') / sum(action='view') as CTR,
                count(distinct user_id) as Feed_users
                from simulator_20221020.feed_actions
                WHERE or(toDate(time) >= today() - 1,
                    toDate(time) = today() - 7,
                    toDate(time) = today() - 8,
                    toDate(time) = today() - 14,
                    toDate(time) = today() - 15)
                group by toDate(time), toStartOfFifteenMinutes(time) as period
            ) feed
            on feed.date=mes.date and feed.period=mes.period
            order by date, period
        """
        return pandahouse.read_clickhouse(query, connection=connection_to_work_db)

    @task
    def checking_metrics(df, metrics):
        for metric in ['Views', 'Likes', 'CTR', 'Feed_users', 'Mes_users', 'Sent_messages']:
            current_period = df['period'].iloc[-1] #выбираем последний период
            result_dict = checking_by_history(df,metric,current_period) #получаем статистику по дов интервалу

            #если результат метрики не в дов интервале
            if (result_dict[metric]>result_dict['up']) | (result_dict[metric]<result_dict['low']):
                for key in result_dict.keys():
                    df.at[df['period']==result_dict['period'], key] = result_dict[key]
                for i in range(1,102):
                    current_period = df['period'].iloc[-i]
                    result_dict = checking_by_history(df.iloc[0:-i],metric,current_period)
                    for key in result_dict.keys():
                        df.at[df['period']==result_dict['period'], key] = result_dict[key]

                df = rolling_df(df)
                print_problem_metric(df, metric)

    checking_data = getting_data()
    checking_metrics(checking_data, metrics)

chernavin_dag_lesson8 = chernavin_dag_lesson8()
