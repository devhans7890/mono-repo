from scripts.scenario_labeler.config.config_loader import ConfigLoader
from scripts.scenario_labeler.config.logger_setup import setup_logger
import redis
from datetime import datetime, timedelta
from string import Template

logger = setup_logger()

redis_config = ConfigLoader.get('db', env_specific=True)['redis']

redis_client = redis.Redis(
    host=redis_config['host'],
    port=redis_config['port'],
    db=redis_config['db'],
    password=redis_config['password'],
    decode_responses=redis_config['decode_responses']
)

def parse_datetime(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")

def filter_recent(data: list, minutes: int) -> list:
    now = datetime.now()
    return [item for item in data if now - parse_datetime(item['@timestamp']) <= timedelta(minutes=minutes)]

def format_value(template: str, context: dict) -> str:
    try:
        return Template(template).substitute(**context)
    except KeyError as e:
        missing_key = str(e).strip("'")
        print(f"[format_value] KeyError: '{missing_key}' is missing in context.\nAvailable keys: {list(context.keys())}")
        raise