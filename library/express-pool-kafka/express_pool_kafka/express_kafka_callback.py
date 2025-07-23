from .express_kafka_produce_record import ExpressKafkaProduceRecord
from express_utils.express_logger_factory import ExpressLoggerFactory
from typing import Optional, Callable, Any
import logging


class ExpressKafkaCallback:
    def __init__(self,
                 kafka_record: ExpressKafkaProduceRecord,
                 # on_success: Optional[Callable[[Any], None]] = None,
                 on_error: Optional[Callable[[Exception], None]] = None):
        self._logger = ExpressLoggerFactory.get_logger(self.__class__.__name__)
        self._kafka_record = kafka_record
        self._on_error = on_error if on_error is not None else self._default_on_error

    def _on_success_callback(self, record_metadata):
        if self._logger.isEnabledFor(logging.TRACE):
            self._logger.trace("Send Success:")
            self._logger.trace(f"Topic: {record_metadata.topic}")
            self._logger.trace(f"Partition: {record_metadata.partition}")
            self._logger.trace(f"Offset: {record_metadata.offset}")
        if self.on_success:
            self.on_success(record_metadata)

    def _default_on_error(self, exception: Exception):
        self._kafka_record.fail()
        if self._logger.isEnabledFor(logging.ERROR):
            self._logger.error(f'Kafka sending exception in callback: {exception} -> {self._kafka_record.data()}')
