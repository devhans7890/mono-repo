from express_utils.express_yml_converter import ExpressYmlConverter
import json

if __name__ == "__main__":
    input_dict = {
        'dot.property': 'value',
        'camelPropertyName': {
            'nested.property.name': 'value',
            'nestedCamelPropertyName': 'value'
        },
        'listCamelDicts': [
            {'inner.dot.property.name': 'value'},
            {'innerCamelPropertyName': 'value'}
        ]
    }

    snake_case_dict = ExpressYmlConverter.dict_keys_to_snake_case(input_dict)
    print(json.dumps(snake_case_dict, indent=2))
