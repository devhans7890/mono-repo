# 데이터 기술통계를 작성하는 방법이다.
# 직접 분석하지 말자
# 단 이때도 unique한 필드는 꼭 제외해 주도록 하자.(@id, 고객번호, 맥주소, ...)

import pandas as pd

df = pd.read_csv("Base.csv")

# 기본 통계 요약
summary = df.describe(include='all').transpose()

# 결측치 개수 추가
summary["missing_count"] = df.isnull().sum()
summary["missing_rate"] = summary["missing_count"] / len(df)

# 고유값 개수 (범주형 변수용)
summary["nunique"] = df.nunique()

# 정리된 요약 보기
pd.set_option("display.max_rows", None)  # 전체 row 보기
print(summary)

from pandas_profiling import ProfileReport  # ydata-profiling으로 변경된 경우에도 동일

profile = ProfileReport(df, title="BAF Dataset Report", explorative=True)
profile.to_file("baf_profile_report.html")

def bin_summary(series, bins=10):
    return pd.cut(series, bins).value_counts().sort_index()

# 예: income 컬럼을 10개 구간으로 나눠 bin count 출력
print(bin_summary(df['income'], bins=10))