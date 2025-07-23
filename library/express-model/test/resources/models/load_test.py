from pycaret.classification import load_model, predict_model
import pandas as pd

# 모델 파일 경로
model_file = 'MODL_SL_TEST_2024-06-05_14_55_31'

# 모델 파일 로드
model = load_model(model_file)

# 모델 파이프라인의 각 단계를 출력
for step_name, step in model.named_steps.items():
    print(f"Step name: {step_name}")
    print(step)
    print("-------")

print("===========================================================")
# 각 단계에서 사용된 변수 추출
numerical_features = model.named_steps['numerical_imputer'].include
categorical_features = model.named_steps['categorical_imputer'].include

# 모든 변수 출력
all_features = list(numerical_features) + list(categorical_features)

print("모델에 사용된 모든 변수들:")
print(all_features)

print("===========================================================")
# 새로운 데이터
new_data = pd.DataFrame({
    'cont_001': [0.1, 0.2, None],  # 일부 필드만 포함
    'cont_002': [0.3, 0.4, 0.5],
    'nominal_043': ['A', 'B', None],  # 일부 필드만 포함
    'nominal_098': ['X', 'Y', 'Z'],
    'nominal_099': ['A', 'B', None]
})

# 누락된 열을 자동으로 추가하고 기본값을 설정하는 함수
def add_missing_columns(df, columns):
    for col in columns:
        if col not in df.columns:
            df[col] = None  # 기본값을 None으로 설정 (또는 다른 적절한 기본값 사용)
    return df

# 새로운 데이터에 누락된 열 추가
new_data = add_missing_columns(new_data, all_features)

# 예측 수행 (전처리 자동 적용)
predictions = predict_model(model, data=new_data)


#  요건
#  누락된 열을 자동으로 추가하고 기본값을 설정하는 함수를 추가하여 에러 제거


print(predictions)