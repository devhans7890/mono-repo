from express_model.express_model_metadata import ExpressModelMetadata
import pandas as pd
import yaml
from tabulate import tabulate

if __name__ == "__main__":
    model_mapping_yml = "resources/model_mapping.yml"

    with open(model_mapping_yml, "r") as file:
        yaml_data = file.read()
        parsed_data = yaml.load(yaml_data, Loader=yaml.FullLoader)

    unsupervised_config = parsed_data["unsupervised"][0]
    metadata = ExpressModelMetadata()
    metadata.build(config=unsupervised_config)

    experiment = metadata.create_experiment()
    model = metadata.load_model(experiment)
    print(model.named_steps['trained_model'])

    # 새로운 데이터를 받았다고 가정
    data = pd.DataFrame({
        'cont_001': [0.1, 0.2, None],
        'cont_002': [0.3, 0.4, 0.5],
        'nominal_043': ['A', 'B', None],
        'nominal_098': ['X', 'Y', 'Z'],
        'nominal_099': ['A', 'B', 'C']
    })


    # 누락된 열을 자동으로 추가하고 기본값을 설정하는 함수
    def add_missing_columns(df, columns):
        for col in columns:
            if col not in df.columns:
                df[col] = None  # 기본값을 None으로 설정 (또는 다른 적절한 기본값 사용)
        return df

    # 새로운 데이터에 누락된 열 추가
    new_data = add_missing_columns(data, model.feature_names_in_)
    predictions = experiment.predict_model(model, data=new_data)
    # parsed_data의 id 값을 기준으로 컬럼명을 변형
    model_id = unsupervised_config['id']
    predictions = predictions.rename(columns={
        'Anomaly': f'{model_id}_Anomaly',
        'Anomaly_Score': f'{model_id}_Anomaly_Score'
    })
    #
    # # 예쁘게 출력
    print(tabulate(predictions, headers='keys', tablefmt='grid'))