from express_pool_kafka.express_kafka_metadata import ExpressKafkaMetadata
from express_pool_kafka.express_kafka_types import ExpressKafkaTypes
import time
import yaml
import json

if __name__ == "__main__":
    file_path = "C:/dfinder_python/py-express-parent/py-express-library/express-pool-kafka/test/resources/pool.yml"

    # Open and parse the YAML file
    with open(file_path, "r") as file:
        yaml_data = file.read()
        parsed_data = yaml.load(yaml_data, Loader=yaml.FullLoader)
    consumer_config = parsed_data['consumer'][0]

    metadata = ExpressKafkaMetadata(ExpressKafkaTypes.CONSUMER)
    metadata.build(_input=consumer_config)
    c = metadata.create_consumer()
    c.subscribe(topics=['test_event'])
    pv_partition = ""
    pv_topic = ""

    try:
        while True:
            # Poll for new messages
            records = c.poll()
            if records is None:
                time.sleep(1)
                continue
            for partition, msgs in records.items():
                for msg in msgs:
                    try:
                        print(partition.partition, type(partition.partition)) # 0 <class 'int'>
                        print(partition) # TopicPartition(topic='test_event', partition=0)
                        print(msg) # ConsumerRecord(topic='test_event', partition=0, offset=7268413, timestamp=1718853896793, timestamp_type=0, key='key', value={'k': 'v'}, headers=[], checksum=None, serialized_key_size=3, serialized_value_size=10, serialized_header_size=-1)
                        print(msg.value, type(msg.value))
                        print("--------------------------------------------------------")
                        print("--------------------------------------------------------")
                        end_offsets = c.end_offsets([partition])

                        print(end_offsets, type(end_offsets))
                        last_offset = end_offsets[partition]

                        lag = last_offset - msg.offset - 1  # end_offsets returns the next offset to be written, so subtract 1
                        print(f"Lag for partition {partition.partition}: {lag}")
                        print("========================================================")
                    except Exception as e:
                        print(e)
                        continue
    except KeyboardInterrupt:
        print("keyboard interrupted")
        c.close()
