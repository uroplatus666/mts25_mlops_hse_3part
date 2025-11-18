import csv
import json
import time
import os
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'transactions_topic'
DATA_DIR = 'data'


def json_serializer(data):
    return json.dumps(data, ensure_ascii=False).encode('utf-8')

def clean_line_generator(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line.startswith('"') and line.endswith('"'):
                line = line[1:-1].replace('""', '"')
            yield line


def get_first_csv_file(directory):
    """Ищет первый CSV файл в указанной директории"""
    if not os.path.exists(directory):
        raise FileNotFoundError(f"Папка '{directory}' не найдена.")

    # Получаем список всех файлов в папке
    files = os.listdir(directory)

    # Фильтруем только .csv
    csv_files = [f for f in files if f.endswith('.csv')]

    if not csv_files:
        raise FileNotFoundError(f"В папке '{directory}' нет .csv файлов.")

    return os.path.join(directory, csv_files[0])


def run_producer():
    print(f"Connecting to Kafka at {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=json_serializer
        )
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return

    try:
        csv_path = get_first_csv_file(DATA_DIR)
    except Exception as e:
        print(f"Error: {e}")
        return

    print(f"Reading and cleaning data from {csv_path}...")

    # Передаем найденный путь (csv_path) в генератор
    reader = csv.DictReader(clean_line_generator(csv_path))
    total_sent = 0

    try:
        for row in reader:
            try:
                row['population_city'] = int(float(row.get('population_city') or 0))
            except ValueError:
                row['population_city'] = 0

            try:
                row['target'] = int(float(row.get('target') or 0))
            except ValueError:
                row['target'] = 0

            for field in ['amount', 'lat', 'lon', 'merchant_lat', 'merchant_lon']:
                val = row.get(field)
                if val and val.strip():
                    try:
                        row[field] = float(val)
                    except ValueError:
                        row[field] = 0.0
                else:
                    row[field] = 0.0

            # 2. Отправка в Kafka
            producer.send(TOPIC_NAME, row)

            total_sent += 1
            if total_sent % 5000 == 0:
                print(f"Sent {total_sent} records so far...")
                producer.flush()

        producer.flush()
        print(f"Done! Total records sent: {total_sent}")

    except FileNotFoundError:
        print(f"Error: File {csv_path} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'producer' in locals():
            producer.close()
        print("Producer closed.")


if __name__ == "__main__":
    run_producer()