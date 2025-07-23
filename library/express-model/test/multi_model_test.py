import dataclasses
from express_model.express_model_metadata import ExpressModelMetadata
import pandas as pd
import yaml
from tabulate import tabulate


def add_missing_columns(df, columns):
    # 새로운 DataFrame을 원본 DataFrame의 복사본으로 생성
    new_df = df.copy()
    # 필요한 열을 추가
    for col in columns:
        if col not in new_df.columns:
            new_df[col] = None  # 기본값을 None으로 설정
    return new_df


@dataclasses.dataclass
class model_vo():
    model_id: str
    model_nm: str
    model_desc: str
    supervised: str


if __name__ == "__main__":
    # model_mapping.yml 파일 경로
    model_mapping_yml = "resources/model_mapping.yml"

    # YAML 파일 읽기
    with open(model_mapping_yml, "r") as file:
        yaml_data = file.read()
        parsed_data = yaml.load(yaml_data, Loader=yaml.FullLoader)

    # test용
    # id: hybird 밑에 superviesd, unsupervised모델 1개씩 등록
    model_dict = {key: value for key, value in parsed_data.items() if key.startswith("hybrid")}

    # 새로운 데이터프레임 예제 (이 데이터로 모든 모델에 대해 예측을 수행)
    origin_data = pd.DataFrame({
        'cont_001': [0.1, 0.2, None],
        'cont_002': [0.3, 0.4, 0.5],
        'nominal_043': ['A', 'B', None],
        'nominal_098': ['X', 'Y', 'Z'],
        'nominal_099': ['A', 'B', None]
    })
    temp_data = origin_data.copy()

    # 각 hybrid 섹션에 대해 예측 수행
    for hybrid_key, hybrid_configs in model_dict.items():
        for config in hybrid_configs:
            if config["model_type"].lower() == "unsupervised":
                metadata = ExpressModelMetadata()
                metadata.build(config=config)
                experiment = metadata.create_experiment()
                model = metadata.load_model(experiment)
                print(experiment)
                print(metadata.get_configuration().model_path)

                # 새로운 데이터에 누락된 열 추가
                unsupervised_feature = add_missing_columns(origin_data, model.feature_names_in_)
                unsupervised_predictions = experiment.predict_model(model, data=unsupervised_feature)
                selected_columns = ['Anomaly', 'Anomaly_Score']
                filtered_predictions = unsupervised_predictions[selected_columns]
                # 비지도학습 결과 추가
                temp_data = pd.concat([temp_data, filtered_predictions], axis=1)
                # id 를 넣어서 정리
                origin_data = pd.concat([origin_data, filtered_predictions], axis=1)
                origin_data = origin_data.rename(columns={
                    'Anomaly': f'{config["id"]}_Anomaly',
                    'Anomaly_Score': f'{config["id"]}_Anomaly_Score',
                })

        for config in hybrid_configs:
            if config["model_type"].lower() == "supervised":
                metadata = ExpressModelMetadata()
                metadata.build(config=config)
                experiment = metadata.create_experiment()
                model = metadata.load_model(experiment)
                print(experiment.__str__())
                print(metadata.get_configuration().model_path)

                # 새로운 데이터에 누락된 열 추가
                supervised_feature = add_missing_columns(temp_data, metadata.get_configuration().feature_list)
                supervised_predictions = experiment.predict_model(model, data=supervised_feature)
                selected_columns = ['prediction_label', 'prediction_score']
                filtered_predictions = supervised_predictions[selected_columns]
                # 지도학습 결과 병합
                temp_data = pd.concat([temp_data, filtered_predictions], axis=1)

                origin_data = pd.concat([origin_data, filtered_predictions], axis=1)
                origin_data = origin_data.rename(columns={
                    'prediction_label': f'{config["id"]}_prediction_label',
                    'prediction_score': f'{config["id"]}_prediction_score',
                })
        temp_data = temp_data.rename(columns={
            'Anomaly': f'{hybrid_key}_Anomaly',
            'Anomaly_Score': f'{hybrid_key}_Anomaly_Score',
            'prediction_label': f'{hybrid_key}_prediction_label',
            'prediction_score': f'{hybrid_key}_prediction_score',
        })
        # 최종 결과 출력
        print(f"Results for {hybrid_key}:")
        print(tabulate(temp_data, headers='keys', tablefmt='grid'))
        print(tabulate(origin_data, headers='keys', tablefmt='grid'))
