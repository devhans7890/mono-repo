import pandas as pd
import requests

# CSV_FILE = "fraud_only_data_2.csv"
CSV_FILE = "normal_only_data_2.csv"
URL = "http://192.168.124.250:1902/concurrent?pretty"

# CSV를 pandas로 읽음 → 각 컬럼의 dtype 자동 추론
df = pd.read_csv(CSV_FILE)

# 필드명 변경: cus_no → customerNo
if "cus_no" in df.columns:
    df = df.rename(columns={"cus_no": "customerNo"})

# 각 row를 dict로 변환하여 JSON POST 요청
for row in df.to_dict(orient="records"):
    try:
        response = requests.post(URL, json=row)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
