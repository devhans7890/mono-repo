from express_configuration.express_configurations import ExpressConfigurations
import logging
import time
from express_configuration.express_configuration_type import ExpressConfigurationType


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = ExpressConfigurations("resources/static.yml", ExpressConfigurationType.DYNAMIC)
    try:
        while True:
            print(config)
            time.sleep(5)
    except KeyboardInterrupt:
        print("Stopped watching configuration file.")
