from watchdog.events import FileSystemEventHandler
from .express_configuration_if import ExpressConfigurationIF


class ConfigFileHandler(FileSystemEventHandler):
    def __init__(self, express_configurations: ExpressConfigurationIF):
        super().__init__()
        self.express_configurations = express_configurations

    def on_modified(self, event):
        if event.src_path.endswith(self.express_configurations.file_path):
            self.express_configurations.reload()
