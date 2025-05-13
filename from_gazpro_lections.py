import pymysql
import pika
import json
import time
from datetime import datetime

# Конфигурация MySQL
MYSQL_CONFIG = {
    'host': '10.10.10.78',
    'user': 'dmntmn',
    'password': 'admpwd',
    'database': 'gazpro2',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# Конфигурация RabbitMQ
RABBITMQ_CONFIG = {
    'host': '10.10.10.11',
    'port': 5672,
    'username': 'bunny',
    'password': 'bunny',
    'exchange': 'questions_exchange',
    'routing_key': 'gazpro_content_lections'
}

def get_mysql_connection():
    """Создает и возвращает соединение с MySQL"""
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        return conn
    except pymysql.Error as err:
        print(f"Ошибка подключения к MySQL: {err}")
        return None

def create_rabbitmq_connection():
    """Создает и возвращает соединение с RabbitMQ"""
    try:
        credentials = pika.PlainCredentials(
            RABBITMQ_CONFIG['username'], 
            RABBITMQ_CONFIG['password']
        )
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_CONFIG['host'],
            port=RABBITMQ_CONFIG['port'],
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        return connection
    except Exception as e:
        print(f"Ошибка подключения к RabbitMQ: {e}")
        return None

def fetch_and_send_data(batch_size=100, delay=1):
    mysql_conn = get_mysql_connection()
    if not mysql_conn:
        return

    rabbit_conn = create_rabbitmq_connection()
    if not rabbit_conn:
        mysql_conn.close()
        return

    channel = rabbit_conn.channel()
    total_sent = 0
    offset = 0
    result = {'temp_id':'', 'title':'', 'description':'', 'pages':[], 'is_draft': False, 'is_archived': False, 'hours': 0,'author_id': '550e8400-e29b-41d4-a716-446655440000', 'educ_center_id': '7d4e42cd-7c41-4e01-a899-e241f4a62eb0' }
    try:
        with mysql_conn.cursor() as cursor:
            while True:
                # Выборка данных с пагинацией
                query = f"""
select ml.id as l_id, ml.title, ml.annotation, lp.ord as ord, mp.id as page_id, mp.content, mp.title as page_title from model_lectures ml
join lectures_pages lp on ml.id=lp.lecture_id
join model_pages mp on mp.id = lp.page_id
order by ml.id , lp.id
LIMIT %s OFFSET %s  
"""

                cursor.execute(query, (batch_size,offset))
                rows = cursor.fetchall()

                if not rows:
                    print("Все данные отправлены")
                    break

                # Отправка каждой строки в RabbitMQ
                for row in rows:

                    if row.get('l_id') != result.get('temp_id'):
                        channel.basic_publish(
                            exchange=RABBITMQ_CONFIG['exchange'],
                            routing_key=RABBITMQ_CONFIG['routing_key'],
                            body=json.dumps(result, ensure_ascii=False),
                            properties=pika.BasicProperties(
                                delivery_mode=2,  # persistent message
                                content_type='application/json'
                            )
                        )
                        # print (result)
                        result.update({'temp_id':row.get('l_id')})
                        result.update({'title':row.get('title')})
                        result.update({'description':row.get('annotation')})
                        result.update({'pages':[]})

                   
                    pages = result.get('pages')
                    pages.append ({'temp_id':row.get('page_id'), 'elements_order':row.get('ord'),  'title':row.get('page_title'),'content':row.get('content')})
                    result.update({'pages':pages})
                    # print (result)

                    total_sent += 1

                print(f"Отправлено {len(rows)} записей. Всего отправлено: {total_sent}")
                offset += batch_size
                time.sleep(delay)

    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        mysql_conn.close()
        rabbit_conn.close()
        print("Соединения закрыты")

if __name__ == "__main__":
    # Параметры

    BATCH_SIZE = 100  # Размер пачки для выборки
    DELAY = 0.05  # Задержка между отправками в секундах

    fetch_and_send_data(BATCH_SIZE, DELAY)