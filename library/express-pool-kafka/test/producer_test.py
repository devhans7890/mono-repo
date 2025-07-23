import time
import yaml
from express_pool_kafka.express_kafka_metadata import ExpressKafkaMetadata
from express_pool_kafka.express_kafka_types import ExpressKafkaTypes
from express_pool_kafka.express_kafka_produce_record import ExpressKafkaProduceRecord
from express_pool_kafka.express_kafka_callback import ExpressKafkaCallback
import json

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

    record = ExpressKafkaProduceRecord("test_event", {"k": "v"}, "key")
    callback = ExpressKafkaCallback(record)
    try:
        while True:
            p.send(record, callback=callback)
            time.sleep(1)
            print("sleep 1")
    except KeyboardInterrupt:
        print("keyboard interrupted")
