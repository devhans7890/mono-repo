from rule.loader import load_rules
from pathlib import Path
import pandas as pd
from scripts.labeler.datasource.db.cfg.connection_factory_instance import ConnectionFactoryInstance
if __name__ == "__main__":    # db 커넥션 풀 인스턴스

    def load_field_timestamp_pairs(basis_value_path: Path, timestamp_path: Path) -> pd.DataFrame:
        basis_values = pd.read_csv(basis_value_path, header=None)
        timestamps = pd.read_csv(timestamp_path, header=None)

        if len(basis_values) != len(timestamps):
            raise ValueError("기준필드 값 수와 시점 수가 불일치")

        df = pd.concat([basis_values, timestamps], axis=1)
        df.columns = ['basis_value', 'timestamp']
        return df


    normal_df = load_field_timestamp_pairs(
        Path("resources/normal_sampled_basis_values.txt"),
        Path("resources/normal_sampled_timestamp.txt")
    )
    def common_instance_setup():
        ConnectionFactoryInstance.get_instance()

    common_instance_setup()

    print("hi")

    # 룰 읽어오기 (분석서버의 mariaDB)
    # 룰 yaml로 저장하기
    # c1 사기 거래 시점까지 확인 했으나 차단 되는 경우가 없다면 Fallback 전략으로 마킹
    # c2 사기 거래 시점까지 확인 했을 때 차단 되는 경우가 있다면 차단 시점에 마킹
    # 룰 yaml로 읽기
    # 사기거래 리스트부터 룰에 따른 탐지 시작
    # 정상거래 리스트도 탐지 시켜봄
    # c1 샘플링된 거래 시점까지 확인 하고 차단되는 경우가 없다면 둠
    # c2 샘플링된 거래 시점까지 확인 하고 차단되는 경우가 있다면 마킹
