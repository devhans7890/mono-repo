import unittest
from unittest.mock import patch, MagicMock
from express_configuration.express_configurations import ExpressConfigurations
from express_configuration.express_configuration_type import ExpressConfigurationType
import os

class TestExpressConfigurations(unittest.TestCase):

    @patch('express_configuration.express_configurations.yaml.safe_load')
    @patch('express_configuration.express_configurations.logging.getLogger')
    def setUp(self, mock_getLogger, mock_safe_load):
        self.file_path = 'test.yml'
        self.config_type = ExpressConfigurationType.DYNAMIC
        self.mock_logger = MagicMock()
        mock_getLogger.return_value = self.mock_logger
        self.configurations = ExpressConfigurations(self.file_path, self.config_type)

    # def test_load_config_success(self, mock_safe_load):
    #     mock_safe_load.return_value = {'key1': 'value1', 'key2': 'value2'}
    #     self.configurations.load_config()
    #     self.assertEqual(self.configurations.configuration['key1'], 'value1')
    #     self.assertEqual(self.configurations.configuration['key2'], 'value2')
    #     self.mock_logger.info.assert_called_with(f"Loaded configuration: {'key1': 'value1', 'key2': 'value2'}")

    # def test_load_config_failure(self, mock_safe_load):
    #     mock_safe_load.side_effect = Exception("Failed to load configuration")
    #     with self.assertRaises(Exception):
    #         self.configurations.load_config()
    #     self.mock_logger.error.assert_called_with("Failed to load configuration")

    def test_get_string(self):
        self.configurations.configuration = {'key1': 'value1'}
        result = self.configurations.get_string('key1')
        self.assertEqual(result, 'value1')

    def test_get_string_default(self):
        self.configurations.configuration = {'key1': 'value1'}
        result = self.configurations.get_string('key2', 'default_value')
        self.assertEqual(result, 'default_value')

    def test_get_int(self):
        self.configurations.configuration = {'key1': '42'}
        result = self.configurations.get_int('key1')
        self.assertEqual(result, 42)

    def test_get_int_default(self):
        self.configurations.configuration = {'key1': '42'}
        result = self.configurations.get_int('key2', 100)
        self.assertEqual(result, 100)

    def test_get_list(self):
        self.configurations.configuration = {'key1': 'value1,value2,value3'}
        result = self.configurations.get_list('key1')
        self.assertEqual(result, ['value1', 'value2', 'value3'])

    def test_get_list_default(self):
        self.configurations.configuration = {}
        result = self.configurations.get_list('key2', ['default_value'])
        self.assertEqual(result, ['default_value'])

    def test_get_dict(self):
        self.configurations.configuration = {'key1': 'value1', 'key2': 'value2'}
        result = self.configurations.get_dict('key1')
        self.assertEqual(result, {'key1': 'value1', 'key2': 'value2'})

    def test_get_dict_default(self):
        self.configurations.configuration = {}
        result = self.configurations.get_dict('key2', {'default_key': 'default_value'})
        self.assertEqual(result, {'default_key': 'default_value'})

    def test_get_keys(self):
        self.configurations.configuration = {'key1': 'value1', 'key2': 'value2', 'other_key': 'value'}
        result = list(self.configurations.get_keys('key'))
        self.assertEqual(result, ['key1', 'key2'])

    # def test_set_property(self):
    #     self.configurations.set_property('new_key', 'new_value')
    #     self.assertEqual(self.configurations.configuration['new_key'], 'new_value')

    def test_get_type(self):
        result = self.configurations.get_type()
        self.assertEqual(result, ExpressConfigurationType.DYNAMIC)

    def test_get_file_path(self):
        result = self.configurations.get_file_path()
        self.assertEqual(result, 'C:/dfinder_python/py-express-parent/py-express-library/express-configuration/test/resources/test.yml')

    def test_get_system_properties(self):
        result = self.configurations.get_system_properties()
        self.assertEqual(result, os.environ)

    # def test_save(self):
    #     with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
    #         self.configurations.configuration = {'key1': 'value1', 'key2': 'value2'}
    #         self.configurations.save()
    #         mock_file.assert_called_with(self.file_path, 'w', encoding='utf-8')
    #         mock_file().write.assert_called_with('key1: value1\nkey2: value2\n')

    def test_get_string_list(self):
        self.configurations.configuration = {'key1': 'value1,value2,value3'}
        result = self.configurations.get_string_list('key1')
        self.assertEqual(result, ['value1', 'value2', 'value3'])

    def test_get_integer_list(self):
        self.configurations.configuration = {'key1': '1,2,3'}
        result = self.configurations.get_integer_list('key1')
        self.assertEqual(result, [1, 2, 3])

    def test_get_float_list(self):
        self.configurations.configuration = {'key1': '1.1,2.2,3.3'}
        result = self.configurations.get_float_list('key1')
        self.assertEqual(result, [1.1, 2.2, 3.3])

    def test_get_boolean_list(self):
        self.configurations.configuration = {'key1': 'true,false,true'}
        result = self.configurations.get_boolean_list('key1')
        self.assertEqual(result, [True, False, True])

    def test_get_string_array(self):
        self.configurations.configuration = {'key1': 'value1,value2,value3'}
        result = self.configurations.get_string_array('key1')
        self.assertEqual(result, ['value1', 'value2', 'value3'])

    def test_get_property(self):
        self.configurations.configuration = {'key1': 'value1'}
        result = self.configurations.get_property('key1')
        self.assertEqual(result, 'value1')

    # def test_reload(self, mock_safe_load):
    #     mock_safe_load.return_value = {'key1': 'value1'}
    #     self.configurations.reload()
    #     self.assertEqual(self.configurations.configuration['key1'], 'value1')
    #     self.mock_logger.info.assert_called_with(f"Reloaded configuration from {self.file_path}")


if __name__ == '__main__':
    unittest.main()
