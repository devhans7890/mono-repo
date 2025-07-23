from express_configuration.express_configuration_utils import ExpressConfigurationUtils
import os
import json
import unittest


class TestExpressConfigurationUtils(unittest.TestCase):
    def test_txt_replacement1(self):
        os.environ["qwerty"] = "hello"
        system_properties = ExpressConfigurationUtils.get_environments()
        self.assertEqual(system_properties["QWERTY"], "hello")
        v1 = ExpressConfigurationUtils.text_replace_with_event(os.environ, "QWERT: %{find_this_text:hello_world}",
                                                               "123")
        v2 = ExpressConfigurationUtils.text_replace_with_event(os.environ, "QWERTY: %{QWERTY:hello_world}",
                                                               "123")
        self.assertEqual(v1, "QWERT: hello_world")
        self.assertEqual(v2, "QWERTY: hello")

    def test_txt_replacement2(self):
        config_template = """
        db:
          host: %{DB_HOST:localhost}
          port: %{DB_PORT:1234}
          user: %{DB_USER:admin}
          password: %{DB_PASSWORD:password}
        """

        os.environ["DB_HOST"] = "127.0.0.1"
        os.environ["DB_PORT"] = "3306"
        os.environ["DB_USER"] = "dfinder"
        # os.environ["DB_PASSWORD"] = "1234567890"

        config = ExpressConfigurationUtils.text_replace_with_event(os.environ, config_template, "")

        # 기대 값
        expected_config = """
        db:
          host: 127.0.0.1
          port: 3306
          user: dfinder
          password: password
        """
        self.assertEqual(config.strip(), expected_config.strip())


    def test_path(self):
        path = 'C:/dfinder_python/py-express-parent/py-express-library/express-configuration/test/resources/dynamic.yml'
        self.assertEqual(path, ExpressConfigurationUtils.get_path_file(path))

        path = 'resources/dynamic.yml'
        self.assertEqual(path, ExpressConfigurationUtils.get_path_file(path))

        path = "dynamic.yml"
        self.assertEqual(
            'C:/dfinder_python/py-express-parent/py-express-library/express-configuration/test/resources/dynamic.yml',
            ExpressConfigurationUtils.get_path_file(path))

    def test_yaml_load(self):
        try:
            json_element = ExpressConfigurationUtils.load_yaml_to_json_element(True, "pool.yml")
        except Exception as e:
            print(e)



# unittest 실행
if __name__ == '__main__':
    unittest.main()
