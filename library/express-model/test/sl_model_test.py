from express_model.express_model_metadata import ExpressModelMetadata
import pandas as pd
import yaml
from tabulate import tabulate

if __name__ == "__main__":
    model_mapping_yml = "./resources/model_mapping.yml"

    with open(model_mapping_yml, "r") as file:
        yaml_data = file.read()
        parsed_data = yaml.load(yaml_data, Loader = yaml.FullLoader)
    supervised_config = parsed_data["supervised"][0]

    metadata = ExpressModelMetadata()
    metadata.build(config=supervised_config)

    experiment = metadata.create_experiment()
    model = metadata.load_model(experiment)
    print(model.named_steps['trained_model'])

    # 새로운 데이터를 받았다고 가정
    origin_data = pd.DataFrame({
        'cont_001': [0.1, 0.2, None],  # 일부 필드만 포함
        'cont_002': [0.3, 0.4, 0.5],
        'nominal_043': ['A', 'B', None],  # 일부 필드만 포함
        'nominal_098': ['X', 'Y', 'Z'],
        'nominal_099': ['A', 'B', None]
    })

    # 누락된 열을 자동으로 추가하고 기본값을 설정하는 함수
    def add_missing_columns(df, columns):
        for col in columns:
            if col == 'target':
                continue
            if col not in df.columns:
                df[col] = None  # 기본값을 None으로 설정 (또는 다른 적절한 기본값 사용)
        return df


    origin_data = add_missing_columns(origin_data, model.feature_names_in_)
    predictions = experiment.predict_model(estimator=model, data=origin_data)

    # parsed_data의 id 값을 기준으로 컬럼명을 변형
    model_id = supervised_config['id']
    predictions = predictions.rename(columns={
        'prediction_label': f'{model_id}_prediction_label',
        'prediction_score': f'{model_id}_prediction_score'
    })
    # 예쁘게 출력
    print(tabulate(predictions, headers='keys', tablefmt='grid'))

