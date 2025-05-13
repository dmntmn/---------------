import pika
import psycopg2
import json
from psycopg2 import sql
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация RabbitMQ
RABBITMQ_CONFIG = {
    'host': '10.10.10.11',
    'port': 5672,
    'username': 'bunny',
    'password': 'bunny',
    'queue': 'gazpro_content_briefings'
}

# Конфигурация PostgreSQL
POSTGRES_CONFIG = {
    'host': '10.1.1.5',
    'port': 5432,
    'user': 'dmntmn',
    'password': 'admpwd',
    'database': 'education_program_management',
}

def get_postgres_connection():
    """Создает и возвращает соединение с PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG['host'],
            port=POSTGRES_CONFIG['port'],
            user=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password'],
            database=POSTGRES_CONFIG['database']
        )
        return conn
    except psycopg2.Error as err:
        logger.error(f"Ошибка подключения к PostgreSQL: {err}")
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
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )
        connection = pika.BlockingConnection(parameters)
        return connection
    except Exception as e:
        logger.error(f"Ошибка подключения к RabbitMQ: {e}")
        return None

def process_message(ch, method, properties, body, postgres_conn):
    """Обрабатывает сообщение из RabbitMQ и записывает в PostgreSQL"""
    try:
        # Парсинг JSON сообщения
        data = json.loads(body)
        logger.info(f"Получено сообщение: {data}")
        old_id = int (data['old_briefing_id'])
        description = data['title']
        query = """
        INSERT INTO public.briefings (temp_id, educ_center_id, title, description,  author_id, is_draft, is_archived)
        VALUES (%s,%s,%s,%s,%s,%s,%s)  RETURNING id
        """

        briefing_question = """
        INSERT INTO public.briefings_questions (briefing_id, question_id, elements_order)
        VALUES (%s,%s,%s)
        """
        # Выполнение запроса
        with postgres_conn.cursor() as cursor:
            cursor.execute(query, (old_id,data['educ_center_id'],'Вопросы для самопроверки',description , data['author_id'], False, False))

            result = cursor.fetchone()
            if result is not None:
                
                inserted_id = result[0]

                a = data['questions']

            i=0
            for one_page in a:

                get_one_q = """
                SELECT id FROM public.questions WHERE temp_id = %s
                """

                cursor.execute(get_one_q, (int(one_page['old_question_id']),))
                # print ('=====',int(one_page['old_question_id']),'1======')
                get_question_id = cursor.fetchone()[0]
                # print ('=====',get_question_id,'2======')
                question_uuid = get_question_id
                # print ('=====',int(one_page['old_question_id']),'3======')
                cursor.execute(briefing_question,(inserted_id, question_uuid, int(i)))
                i=i+1
            postgres_conn.commit()
        
        logger.info("Данные успешно записаны в PostgreSQL")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except psycopg2.Error as e:
        logger.error(f"Ошибка записи в PostgreSQL: {e}")
        postgres_conn.rollback()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def consume_messages():
    """Основная функция для потребления сообщений"""
    postgres_conn = get_postgres_connection()
    if not postgres_conn:
        return

    rabbit_conn = create_rabbitmq_connection()
    if not rabbit_conn:
        postgres_conn.close()
        return

    try:
        channel = rabbit_conn.channel()
        
        # Настройка QoS (качество обслуживания)
        channel.basic_qos(prefetch_count=1)
        
        # Подписка на очередь
        channel.basic_consume(
            queue=RABBITMQ_CONFIG['queue'],
            on_message_callback=lambda ch, method, properties, body: 
                process_message(ch, method, properties, body, postgres_conn),
            auto_ack=False
        )       
        logger.info("Ожидание сообщений. Для выхода нажмите CTRL+C")
        channel.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания. Завершение работы...")
    except Exception as e:
        logger.error(f"Ошибка в работе потребителя: {e}")
    finally:
        if rabbit_conn and rabbit_conn.is_open:
            rabbit_conn.close()
        if postgres_conn and not postgres_conn.closed:
            postgres_conn.close()
        logger.info("Соединения закрыты")

if __name__ == "__main__":
    consume_messages()