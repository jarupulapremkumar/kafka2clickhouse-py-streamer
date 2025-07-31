import sys,os
from clickhouse_connect import get_client
from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException
from dotenv import load_dotenv

def check_clickhouse(host,username,password):
    try:
        client = get_client(host=host, port=8123, username=username, password=password)
        result = client.query('SELECT 1')
        return result.result_rows[0][0] == 1
    except Exception as e:
        print(f"ClickHouse check failed: {e}")
        return False

def check_kafka(broker):
    try:
        admin_client = AdminClient({'bootstrap.servers': broker})
        metadata = admin_client.list_topics(timeout=5)
        return bool(metadata.topics)
    except KafkaException as ke:
        print(f"Kafka check failed: {ke}")
        return False
    except Exception as e:
        print(f"Kafka error: {e}")
        return False

if __name__ == "__main__":
    
    KAFKA_BROKER=  os.environ.get("KAFKA_BROKER")
    CLICKHOUSE_HOST =  os.environ.get("CLICKHOUSE_HOST")
    CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE")
    CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER")
    CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
            
        
    # Load environment variables from .env file if it exists
    if KAFKA_BROKER is None:
        load_dotenv()
        
        KAFKA_BROKER = os.getenv("KAFKA_BROKER")
        CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
        CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE")
        CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
        CLICKHOUSE_PASSWORD =  os.getenv("CLICKHOUSE_PASSWORD")

    if check_clickhouse(CLICKHOUSE_HOST,CLICKHOUSE_USER,CLICKHOUSE_PASSWORD) and check_kafka(KAFKA_BROKER):
        sys.exit(0)  # Ready
    else:
        sys.exit(1)  # Not ready
