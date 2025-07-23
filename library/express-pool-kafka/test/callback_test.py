from kafka.errors import KafkaTimeoutError
import json
import time
import yaml
from express_pool_kafka.express_kafka_metadata import ExpressKafkaMetadata
from express_pool_kafka.express_kafka_types import ExpressKafkaTypes
from express_pool_kafka.express_kafka_produce_record import ExpressKafkaProduceRecord
from express_pool_kafka.express_kafka_callback import ExpressKafkaCallback

if __name__ == "__main__":
    file_path = "C:/dfinder_python/py-express-parent/py-express-library/express-pool-kafka/test/resources/pool.yml"

    # Open and parse the YAML file
    with open(file_path, "r") as file:
        yaml_data = file.read()
        parsed_data = yaml.load(yaml_data, Loader=yaml.FullLoader)
    producer_config = parsed_data['producer'][0]

    metadata = ExpressKafkaMetadata(ExpressKafkaTypes.PRODUCER)
    metadata.build(_input=producer_config)
    p = metadata.create_producer()

    record = ExpressKafkaProduceRecord("test_event", {"k":"v"},"key")
    # 토픽이 생성되지 않았다면 에러가 날것
    callback = ExpressKafkaCallback(record)
    try:
        while True:
            try:
                p.send(record, callback=callback)
                time.sleep(1)
                print("sleep 1")
            except KafkaTimeoutError as kte:
                print(f"Kafka timeout error: {kte}")
    except KeyboardInterrupt:
        print("keyboard interrupted")
