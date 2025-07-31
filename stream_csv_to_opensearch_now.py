import pandas as pd
from opensearchpy import OpenSearch, helpers
import os
import datetime
import time

host = 'localhost'
port = 9200
username = 'admin'
password = 'StrongPassword22!' # 실제 설정한 강력한 비밀번호로 변경하세요! (예: P@ssw0rd1234)

client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_auth=(username, password),
    use_ssl=True,
    verify_certs=False, # 개발 환경에서 자가 서명 인증서 사용 시 False로 설정하면 에러 발생 가능성 있음 (주의: 프로덕션 환경에서는 True 권장)
    ssl_assert_hostname=False,
    ssl_show_warn=False
)

index_name = 'sensor_data_stream'
csv_file_path = 'sensor_data.csv'

def create_index_if_not_exists():
    """인덱스가 없으면 생성하고, 매핑을 정의합니다."""
    # exists() 메서드는 키워드 인자 'index'를 사용해야 합니다.
    if client.indices.exists(index=index_name):
        print(f"Index '{index_name}' already exists. Deleting and recreating for fresh stream simulation...")
        client.indices.delete(index=index_name) # delete() 메서드도 키워드 인자 'index'를 사용합니다.

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
    client.indices.create(index=index_name, body=index_body) # create() 메서드도 키워드 인자 'index'를 사용합니다.
    print(f"Index '{index_name}' created or recreated.")

def stream_csv_data_to_opensearch(file_path):
    """CSV 파일을 실시간 스트림처럼 읽어 OpenSearch로 적재합니다. timestamp는 현재 시간으로 설정됩니다."""
    if not os.path.exists(file_path):
        print(f"Error: CSV file not found at '{file_path}'")
        return

    print(f"Starting to stream data from CSV file: {file_path}")

    df = pd.read_csv(file_path)
    # CSV의 timestamp 컬럼은 더 이상 사용하지 않고, 현재 시간을 사용합니다.
    # df['timestamp'] = pd.to_datetime(df['timestamp']) # 이 줄은 이제 필요 없습니다.
    
    # 시간 순서로 정렬하는 것은 현재 timestamp를 사용하기 때문에 의미가 없어지지만,
    # 원본 CSV의 id 순서나 다른 필드를 기준으로 데이터의 순서를 유지하고 싶다면 유지할 수 있습니다.
    # 여기서는 CSV 파일의 원본 순서대로 처리하도록 정렬 로직을 제거하거나 필요에 따라 조정할 수 있습니다.
    # df = df.sort_values(by='timestamp').reset_index(drop=True) # 이 줄은 이제 필요 없습니다.

    print(f"Total {len(df)} records to stream.")

    for index, row in df.iterrows():
        # 현재 시간을 가져와 timestamp 필드에 할당
        current_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        doc = {
            'id': str(row['id']),
            'sensor_id': str(row['sensor_id']),
            'zone_id': str(row['zone_id']),
            'timestamp': current_timestamp, # CSV의 timestamp 대신 현재 시간을 사용합니다!
            'sensor_type': str(row['sensor_type']),
            'unit': str(row['unit']),
            'val': float(row['val'])
        }

        try:
            response = client.index(index=index_name, body=doc)
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Inserted document (ID: {response['_id']}): Sensor: {doc['sensor_id']}, Type: {doc['sensor_type']}, Value: {doc['val']} at {doc['timestamp']}")
        except Exception as e:
            print(f"Error inserting document at row {index}: {e}")

        time.sleep(1) # 1초 간격으로 데이터 삽입

if __name__ == '__main__':
    create_index_if_not_exists()
    stream_csv_data_to_opensearch(csv_file_path)
    print("CSV data streaming simulation complete.")