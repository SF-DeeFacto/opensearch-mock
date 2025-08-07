import pandas as pd
from opensearchpy import OpenSearch
import os
import datetime
import time
import threading

# OpenSearch 설정
HOST = 'localhost'
PORT = 9200
USERNAME = 'admin'
PASSWORD = 'StrongPassword22!'

client = OpenSearch(
    hosts=[{'host': HOST, 'port': PORT}],
    http_auth=(USERNAME, PASSWORD),
    use_ssl=True,
    verify_certs=False,  # 개발 환경에서만 False 허용
    ssl_assert_hostname=False,
    ssl_show_warn=False
)

# 인덱스 생성 함수
def create_index(index_name: str, index_body: dict):
    """기존 인덱스를 삭제 후 재생성"""
    if client.indices.exists(index=index_name):
        print(f"Index '{index_name}' already exists. Deleting and recreating...")
        client.indices.delete(index=index_name)
    client.indices.create(index=index_name, body=index_body)
    print(f"Index '{index_name}' created.")

# 센서 데이터 스트리밍 함수
def stream_csv_data_to_opensearch(file_path: str):
    """CSV 데이터를 실시간처럼 OpenSearch로 전송 (일반 센서)"""
    if not os.path.exists(file_path):
        print(f"Error: CSV file not found at '{file_path}'")
        return

    print(f"Streaming from: {file_path}")
    df = pd.read_csv(file_path)
    print(f"Total {len(df)} records to stream.")

    for _, row in df.iterrows():
        current_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        doc = {
            'id': str(row['id']),
            'sensor_id': str(row['sensor_id']),
            'zone_id': str(row['zone_id']),
            'timestamp': current_timestamp,
            'sensor_type': str(row['sensor_type']),
            'unit': str(row['unit']),
            'val': float(row['val'])
        }

        index_name = None
        if row['sensor_type'] == 'temperature':
            index_name = 'temp_sensor_data_stream'
        elif row['sensor_type'] == 'humidity':
            index_name = 'humi_sensor_data_stream'
        elif row['sensor_type'] == 'esd':
            index_name = 'esd_sensor_data_stream'
        elif row['sensor_type'] == 'windDir':
            index_name = 'winddir_sensor_data_stream'

        try:
            # 수정 필요🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥
            response = client.index(index=index_name, body=doc)
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Inserted: {doc['sensor_id']} ({doc['val']})")
        except Exception as e:
            print(f"Error inserting document: {e}")

        time.sleep(1)

def stream_csv_data_to_opensearch_particle(file_path: str):
    """CSV 데이터를 실시간처럼 OpenSearch로 전송 (입자 센서)"""
    if not os.path.exists(file_path):
        print(f"Error: CSV file not found at '{file_path}'")
        return

    print(f"Streaming from: {file_path}")
    df = pd.read_csv(file_path)
    print(f"Total {len(df)} records to stream.")

    for _, row in df.iterrows():
        current_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        doc = {
            'id': str(row['id']),
            'sensor_id': str(row['sensor_id']),
            'zone_id': str(row['zone_id']),
            'timestamp': current_timestamp,
            'sensor_type': str(row['sensor_type']),
            'unit': str(row['unit']),
            'val_0_1': float(row['val_0_1']),
            'val_0_3': float(row['val_0_3']),
            'val_0_5': float(row['val_0_5'])
        }



        try:
            response = client.index(index='particle_sensor_data_stream', body=doc)
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Inserted: {doc['sensor_id']} (0.1μm: {doc['val_0_1']})")
        except Exception as e:
            print(f"Error inserting document: {e}")

        time.sleep(1)

# 메인 실행부 (센서 5개 데이터)
if __name__ == '__main__':
    # 인덱스 매핑 정의
    index_body = {
        'settings': {'index': {'number_of_shards': 1}},
        'mappings': {
            'properties': {
                'id': {'type': 'keyword'},
                'sensor_id': {'type': 'keyword'},
                'zone_id': {'type': 'keyword'},
                'timestamp': {'type': 'date'},
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
                'timestamp': {'type': 'date'},
                'sensor_type': {'type': 'keyword'},
                'unit': {'type': 'keyword'},
                'val_0_1': {'type': 'float'},
                'val_0_3': {'type': 'float'},
                'val_0_5': {'type': 'float'}
            }
        }
    }

    # 인덱스 생성
    create_index('temp_sensor_data_stream', index_body)
    create_index('humi_sensor_data_stream', index_body)
    create_index('esd_sensor_data_stream', index_body)
    create_index('winddir_sensor_data_stream', index_body)
    create_index('particle_sensor_data_stream', particle_index_body)

    # CSV 파일 목록 정의
    csv_files = [
        ('./data/sensor_data.csv', stream_csv_data_to_opensearch),
        ('./data/temp2_meta.csv', stream_csv_data_to_opensearch),
        ('./data/humi_meta.csv', stream_csv_data_to_opensearch),
        ('./data/humi2_meta.csv', stream_csv_data_to_opensearch),
        ('./data/lpm1_meta.csv', stream_csv_data_to_opensearch_particle),
        ('./data/lpm2_meta.csv', stream_csv_data_to_opensearch_particle),
        ('./data/wd1_meta.csv', stream_csv_data_to_opensearch),
        ('./data/wd2_meta.csv', stream_csv_data_to_opensearch),
        ('./data/esd1_meta.csv', stream_csv_data_to_opensearch),
        ('./data/esd2_meta.csv', stream_csv_data_to_opensearch),
    ]

    # 스레드 실행
    threads = []
    for file_path, func in csv_files:
        thread = threading.Thread(target=func, args=(file_path,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    print("✅ All CSV data streaming simulations complete.")
