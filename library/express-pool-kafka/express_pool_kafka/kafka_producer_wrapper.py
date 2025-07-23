from .express_kafka_callback import ExpressKafkaCallback
from .express_kafka_produce_record import ExpressKafkaProduceRecord
from kafka import KafkaProducer
from typing import Optional, Dict, Any


class KafkaProducerWrapper:
    def __init__(self, configs: Dict[str, Any]):
        self.kafka_internal_producer = KafkaProducer(**configs)

    def send(self,
             record: ExpressKafkaProduceRecord,
             callback: Optional[ExpressKafkaCallback] = None):
        data = record.data()
        if data is None:
            raise Exception(" * Kafka produce data is empty")

        topic = record.topic()
        if topic is None:
            raise Exception(" * Kafka produce topic is empty")

        key = record.key()

        future = self.kafka_internal_producer.send(
            topic=topic,
            value=data,
            key=key
        )

        if callback is not None:
            # future.add_callback(callback.on_success_callback)
            future.add_errback(callback._on_error)

        self.kafka_internal_producer.flush()

    def fast_send(self,
             record: ExpressKafkaProduceRecord):
        data = record.data()
        if data is None:
            raise Exception(" * Kafka produce data is empty")

        topic = record.topic()
        if topic is None:
            raise Exception(" * Kafka produce topic is empty")

        self.kafka_internal_producer.send(
            topic=topic,
            value=data
        )
        self.kafka_internal_producer.flush()

    def close(self, timeout: Optional[float] = None):
        if timeout is not None:
            self.kafka_internal_producer.close(timeout)
        else:
            self.kafka_internal_producer.close()

    def to_string(self):
        return str(self.kafka_internal_producer)
