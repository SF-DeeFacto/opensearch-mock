import pandas as pd
from opensearchpy import OpenSearch, helpers
import os
import datetime
import time
import threading

host = 'localhost'
port = 9200
username = 'admin'
password = 'StrongPassword22!'

client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_auth=(username, password),
    use_ssl=True,
    verify_certs=False, # 개발 환경에서 자가 서명 인증서 사용 시 False로 설정하면 에러 발생 가능성 있음 (주의: 프로덕션 환경에서는 True 권장)
    ssl_assert_hostname=False,
    ssl_show_warn=False
)

def create_index(index_name, index_body):
    if client.indices.exists(index=index_name):
        print(f"Index '{index_name}' already exists. Deleting and recreating...")
        client.indices.delete(index=index_name)

    client.indices.create(index=index_name, body=index_body)
    print(f"Index '{index_name}' created.")


def stream_csv_data_to_opensearch(file_path):
    """CSV 파일을 실시간 스트림처럼 읽어 OpenSearch로 적재합니다. timestamp는 현재 시간으로 설정됩니다."""
    if not os.path.exists(file_path):
        print(f"Error: CSV file not found at '{file_path}'")
        return

    print(f"Starting to stream data from CSV file: {file_path}")

    df = pd.read_csv(file_path)

    print(f"Total {len(df)} records to stream.")

    for index, row in df.iterrows():
        # 현재 시간을 가져와 timestamp 필드에 할당
        current_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        doc = {
            'id': str(row['id']),
            'sensor_id': str(row['sensor_id']),
            'zone_id': str(row['zone_id']),
            'timestamp': current_timestamp, # CSV의 timestamp 대신 현재 시간 사용
            'sensor_type': str(row['sensor_type']),
            'unit': str(row['unit']),
            'val': float(row['val'])
        }

        try:
            response = client.index(index='sensor_data_stream', body=doc)
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Inserted document (ID: {response['_id']}): Sensor: {doc['sensor_id']}, Type: {doc['sensor_type']}, Value: {doc['val']} at {doc['timestamp']}")
        except Exception as e:
            print(f"Error inserting document at row {index}: {e}")

        time.sleep(1) # 1초 간격으로 데이터 삽입

def stream_csv_data_to_opensearch_particle(file_path):
    """CSV 파일을 실시간 스트림처럼 읽어 OpenSearch로 적재합니다. timestamp는 현재 시간으로 설정됩니다."""
    if not os.path.exists(file_path):
        print(f"Error: CSV file not found at '{file_path}'")
        return

    print(f"Starting to stream data from CSV file: {file_path}")

    df = pd.read_csv(file_path)

    print(f"Total {len(df)} records to stream.")

    for index, row in df.iterrows():
        # 현재 시간을 가져와 timestamp 필드에 할당
        current_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        doc = {
            'id': str(row['id']),
            'sensor_id': str(row['sensor_id']),
            'zone_id': str(row['zone_id']),
            'timestamp': current_timestamp, # CSV의 timestamp 대신 현재 시간 사용
            'sensor_type': str(row['sensor_type']),
            'unit': str(row['unit']),
            'val_0_1': float(row['val_0_1']),
            'val_0_3': float(row['val_0_3']),
            'val_0_5': float(row['val_0_5'])
        }

        try:
            response = client.index(index='particle_sensor_data_stream', body=doc)
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Inserted document (ID: {response['_id']}): Sensor: {doc['sensor_id']}, Type: {doc['sensor_type']}, Value: {doc['val_0_1']} at {doc['timestamp']}")
        except Exception as e:
            print(f"Error inserting document at row {index}: {e}")

        time.sleep(1) # 1초 간격으로 데이터 삽입


if __name__ == '__main__':
    index_body = {
        'settings': {'index': {'number_of_shards': 1}},
        'mappings': {
            'properties': {
                'id': {'type': 'keyword'},
                'sensor_id': {'type': 'keyword'},
                'zone_id': {'type': 'keyword'},
                'timestamp': {'type': 'date'}, # 현재 시간으로 들어가므로 date 타입 유지
                'sensor_type': {'type': 'keyword'},
                'unit': {'type': 'keyword'},
                'val': {'type': 'float'}
            }
        }
    }

    particle_index_body = {
        'settings': {'index': {'number_of_shards': 1}},
        'mappings': {
            'properties': {
                'id': {'type': 'keyword'},
                'sensor_id': {'type': 'keyword'},
                'zone_id': {'type': 'keyword'},
                'timestamp': {'type': 'date'}, # 현재 시간으로 들어가므로 date 타입 유지
                'sensor_type': {'type': 'keyword'},
                'unit': {'type': 'keyword'},
                'val_0_1': {'type': 'float'},
                'val_0_3': {'type': 'float'},
                'val_0_5': {'type': 'float'}
            }
        }
    }

    create_index('sensor_data_stream', index_body)
    create_index('particle_sensor_data_stream', particle_index_body)

    # 두 개의 CSV 파일 경로
    csv_file_1 = './data/sensor_data.csv'
    csv_file_2 = './data/temp2_meta.csv'

    csv_file_3 = './data/humi_meta.csv'
    csv_file_4 = './data/humi2_meta.csv'

    csv_file_5 = './data/lpm1_meta.csv'
    csv_file_6 = './data/lpm2_meta.csv'

    csv_file_7 = './data/wd1_meta.csv'
    csv_file_8 = './data/wd2_meta.csv'

    csv_file_9 = './data/esd1_meta.csv'
    csv_file_10 = './data/esd2_meta.csv'

    # 각각의 스레드 정의
    thread1 = threading.Thread(target=stream_csv_data_to_opensearch, args=(csv_file_1,))
    thread2 = threading.Thread(target=stream_csv_data_to_opensearch, args=(csv_file_2,))
    thread3 = threading.Thread(target=stream_csv_data_to_opensearch, args=(csv_file_3,))
    thread4 = threading.Thread(target=stream_csv_data_to_opensearch, args=(csv_file_4,))
    thread5 = threading.Thread(target=stream_csv_data_to_opensearch_particle, args=(csv_file_5,))
    thread6 = threading.Thread(target=stream_csv_data_to_opensearch_particle, args=(csv_file_6,))
    thread7 = threading.Thread(target=stream_csv_data_to_opensearch, args=(csv_file_7,))
    thread8 = threading.Thread(target=stream_csv_data_to_opensearch, args=(csv_file_8,))
    thread9 = threading.Thread(target=stream_csv_data_to_opensearch, args=(csv_file_9,))
    thread10 = threading.Thread(target=stream_csv_data_to_opensearch, args=(csv_file_10,))

    # 스레드 시작
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()
    thread7.start()
    thread8.start()
    thread9.start()
    thread10.start()

    # 모든 스레드가 종료될 때까지 대기
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    thread6.join()
    thread7.join()
    thread8.join()
    thread9.join()
    thread10.join()

    print("All CSV data streaming simulations complete.")



