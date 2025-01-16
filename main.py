from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import logging
import numpy as np
import sqlite3  
import os

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://stockter.netlify.app", "http://localhost:3000"],  # 실제 배포 시 특정 도메인으로 제한 권장
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

###############################
# 1) DB에서 데이터 로드 (수정)
###############################

# 프로젝트 루트 디렉토리에 있는 DB 파일을 가리키도록 경로 수정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_PATH = os.path.join(BASE_DIR, "my_stock.db")
ANNUAL_TABLE_NAME = "annual_data"
DART_TABLE_NAME = "stock_data"  
QUARTERLY_TABLE_NAME = "quarterly_data"

def load_annual_data_from_sqlite():
    """
    SQLite DB에서 annual_data 테이블의 데이터를 읽어오고,
    기존 엑셀에서 하던 전처리를 동일하게 수행한 뒤 df를 반환.
    """
    conn = sqlite3.connect(SQLITE_DB_PATH)
    df_local = pd.read_sql(f"SELECT * FROM {ANNUAL_TABLE_NAME}", conn)
    conn.close()

    # NaN -> "N/A" 처리
    df_local = df_local.fillna("N/A")

    # -------------------------------
    # (엑셀 때와 동일한 숫자형 변환 로직)
    # -------------------------------

    # 시가총액을 숫자형으로 변환
    df_local["시가총액(숫자형)"] = df_local["시가총액"].apply(convert_marketcap)

    # 모든 년도 매출액 열 숫자 변환
    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        revenue_column = f"{year} 매출액"
        if revenue_column in df_local.columns:
            df_local[revenue_column] = df_local[revenue_column].apply(convert_revenue)
        else:
            # 없는 열은 0
            df_local[revenue_column] = 0

    # 모든 년도 영업이익 열 숫자 변환
    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        operating_income_column = f"{year} 영업이익"
        if operating_income_column in df_local.columns:
            df_local[operating_income_column] = df_local[operating_income_column].apply(convert_operating_income)
        else:
            df_local[operating_income_column] = 0

    # 모든 년도 순이익률 열 숫자 변환
    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        net_income_rate_column = f"{year} 순이익률"
        if net_income_rate_column in df_local.columns:
            df_local[net_income_rate_column] = df_local[net_income_rate_column].apply(convert_net_income_rate)
        else:
            df_local[net_income_rate_column] = 0

    # 모든 년도 영업이익률 열 숫자 변환
    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        operating_income_rate_column = f"{year} 영업이익률"
        if operating_income_rate_column in df_local.columns:
            df_local[operating_income_rate_column] = df_local[operating_income_rate_column].apply(convert_operating_income_rate)
        else:
            df_local[operating_income_rate_column] = 0

    # 모든 년도 EPS 열 숫자 변환
    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        eps_column = f"{year} EPS(원)"
        if eps_column in df_local.columns:
            df_local[eps_column] = df_local[eps_column].apply(convert_eps)
        else:
            df_local[eps_column] = 0

    # 모든 년도 PER 열 숫자 변환
    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        per_column = f"{year} PER(배)"
        if per_column in df_local.columns:
            df_local[per_column] = df_local[per_column].apply(convert_per)
        else:
            df_local[per_column] = 0

    # 모든 년도 PBR 열 숫자 변환
    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        pbr_column = f"{year} PBR(배)"
        if pbr_column in df_local.columns:
            df_local[pbr_column] = df_local[pbr_column].apply(convert_pbr)
        else:
            df_local[pbr_column] = 0

    # 모든 년도 시가배당률 열 숫자 변환
    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        dividend_yield_column = f"{year} 시가배당률(%)"
        if dividend_yield_column in df_local.columns:
            df_local[dividend_yield_column] = df_local[dividend_yield_column].apply(convert_dividend_yield)
        else:
            df_local[dividend_yield_column] = np.nan

    return df_local

def load_dart_data_from_sqlite():
    """
    SQLite DB에서 stock_data 테이블의 데이터를 읽어오고,
    기존 엑셀에서 하던 전처리를 동일하게 수행한 뒤 dart_df 반환.
    """
    conn = sqlite3.connect(SQLITE_DB_PATH)
    dart_df_local = pd.read_sql(f"SELECT * FROM {DART_TABLE_NAME}", conn)
    conn.close()

    # NaN -> "N/A" 처리
    dart_df_local = dart_df_local.fillna("N/A")

    # -------------------------------
    # 기존 엑셀 로드 시와 동일한 숫자형 변환 로직
    # -------------------------------
    dart_df_local["매출액"] = dart_df_local["매출액"].apply(convert_financial_revenue)
    dart_df_local["영업이익"] = dart_df_local["영업이익"].apply(convert_financial_operating_profit)
    dart_df_local["당기순이익"] = dart_df_local["당기순이익"].apply(convert_financial_net_income)
    dart_df_local["영업이익률"] = dart_df_local.apply(calculate_operating_income_rate, axis=1)
    dart_df_local["부채총계"] = dart_df_local["부채총계"].apply(convert_financial_total_debt)
    dart_df_local["자본총계"] = dart_df_local["자본총계"].apply(convert_financial_total_equity)
    dart_df_local["부채비율"] = dart_df_local.apply(calculate_debt_ratio, axis=1)

    return dart_df_local

# -----------------------------
# 숫자 변환 함수들 (기존 코드 그대로)
# -----------------------------
def convert_marketcap(value):
    if isinstance(value, str):
        # (1) 미리 쉼표 제거
        value = value.replace(",", "").strip()
        if "조" in value:
            parts = value.split("조")
            # 예: "1조 2333" -> parts[0] = "1", parts[1] = "2333"
            trillion = float(parts[0]) * 10**12
            if len(parts) > 1 and parts[1]:
                trillion += float(parts[1]) * 10**8
            return trillion
        elif "억" in value:
            # "1억2333" => 쉼표 제거 후 -> "1억2333"
            # 여기서는 억만 제거하고 float 변환
            return float(value.replace("억", "")) * 10**8
    # 문자열이 아니거나, "조", "억"이 없는 케이스
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def convert_revenue(value):
    if isinstance(value, str):
        if value == "N/A" or value == "-":
            return 0
        value = value.replace(",", "")
        if "억" in value:
            return float(value.replace("억", "").strip()) * 10**8
        elif "조" in value:
            parts = value.split("조")
            trillion = float(parts[0]) * 10**12
            if len(parts) > 1 and parts[1]:
                trillion += float(parts[1].strip()) * 10**8
            return trillion
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0

def convert_operating_income(value):
    if isinstance(value, str):
        if value == "N/A" or value == "-":
            return 0
        value = value.replace(",", "")
        if "억" in value:
            return float(value.replace("억", "").strip()) * 10**8
        elif "조" in value:
            parts = value.split("조")
            trillion = float(parts[0]) * 10**12
            if len(parts) > 1 and parts[1]:
                trillion += float(parts[1].strip()) * 10**8
            return trillion
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0

def convert_net_income_rate(value):
    if isinstance(value, str):
        if value.endswith("%"):
            try:
                return float(value.rstrip("%"))
            except ValueError:
                return 0
        elif value == "N/A" or value == "-":
            return 0
        else:
            try:
                return float(value)
            except ValueError:
                return 0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0

def convert_operating_income_rate(value):
    if isinstance(value, str):
        if value.endswith("%"):
            try:
                return float(value.rstrip("%"))
            except ValueError:
                return 0
        elif value == "N/A" or value == "-":
            return 0
        else:
            try:
                return float(value)
            except ValueError:
                return 0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0

def convert_eps(value):
    if isinstance(value, str):
        value = value.replace("원", "").replace(",", "").strip()
        if value in ["N/A", "-"]:
            return 0
        try:
            return float(value)
        except ValueError:
            return 0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0

def convert_per(value):
    if isinstance(value, str):
        value = value.replace("배", "").replace(",", "").strip()
        if value in ["N/A", "-"]:
            return np.nan
        try:
            return float(value)
        except ValueError:
            return np.nan
    try:
        return float(value)
    except (ValueError, TypeError):
        return np.nan

def convert_pbr(value):
    if isinstance(value, str):
        value = value.replace("배", "").replace(",", "").strip()
        if value in ["N/A", "-"]:
            return np.nan
        try:
            return float(value)
        except ValueError:
            return np.nan
    try:
        return float(value)
    except (ValueError, TypeError):
        return np.nan
    
def convert_roe(value):
    if isinstance(value, str):
        value = value.replace("%", "").replace(",", "").replace("배", "").strip()
        if value in ["N/A", "-", ""]:
            return np.nan
        try:
            return float(value)
        except ValueError:
            return np.nan
    try:
        return float(value)
    except (ValueError, TypeError):
        return np.nan


def convert_dividend_yield(value):
    if isinstance(value, str):
        value = value.strip()
        if value in ["N/A", "-", ""]:
            return np.nan
        if "%" in value:
            try:
                return float(value.replace("%", "").replace(",", "").strip())
            except ValueError:
                return np.nan
    try:
        return float(value)
    except (ValueError, TypeError):
        return np.nan

def convert_financial_revenue(value):
    if isinstance(value, str):
        if value.upper() == "N/A" or value == "-":
            return 0
        value = value.replace(",", "")
        try:
            return float(value)
        except ValueError:
            return 0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0

def convert_financial_operating_profit(value):
    if isinstance(value, str):
        if value.upper() == "N/A" or value.strip() == "-":
            return 0
        value = value.replace(",", "")
        try:
            return float(value)
        except ValueError:
            return 0
    try:
        return float(value) if pd.notnull(value) else 0
    except (ValueError, TypeError):
        return 0

def convert_financial_net_income(value):
    if isinstance(value, str):
        if value.upper() == "N/A" or value.strip() == "-":
            return 0
        value = value.replace(",", "")
        try:
            return float(value)
        except ValueError:
            return 0
    try:
        return float(value) if pd.notnull(value) else 0
    except (ValueError, TypeError):
        return 0

def convert_financial_total_debt(value):
    if isinstance(value, str):
        if value.upper() == "N/A" or value.strip() == "-":
            return 0
        value = value.replace(",", "")
        try:
            return float(value)
        except ValueError:
            return 0
    try:
        return float(value) if pd.notnull(value) else 0
    except (ValueError, TypeError):
        return 0

def convert_financial_total_equity(value):
    if isinstance(value, str):
        if value.upper() == "N/A" or value.strip() == "-":
            return 0
        value = value.replace(",", "")
        try:
            return float(value)
        except ValueError:
            return 0
    try:
        return float(value) if pd.notnull(value) else 0
    except (ValueError, TypeError):
        return 0

# ----------------------------------------
# 전역 annual_df 및 quarterly_df, dart_df (★ DB에서 읽어온 결과 저장)
# ----------------------------------------
annual_df = None
quarterly_df = None
dart_df = None


# ----------------------------------------
# 연도별 컬럼 맵핑 (기존 코드)
# ----------------------------------------
YEAR_TO_REVENUE_COLUMN = {
    "2021": "2021.12 매출액",
    "2022": "2022.12 매출액",
    "2023": "2023.12 매출액",
    "2024": "2024.12 매출액",
}

YEAR_TO_OPERATING_INCOME_COLUMN = {
    "2021": "2021.12 영업이익",
    "2022": "2022.12 영업이익",
    "2023": "2023.12 영업이익",
    "2024": "2024.12 영업이익",
}

YEAR_TO_NET_INCOME_RATE_COLUMN = {
    "2021": "2021.12 순이익률",
    "2022": "2022.12 순이익률",
    "2023": "2023.12 순이익률",
    "2024": "2024.12 순이익률",
}

YEAR_TO_OPERATING_INCOME_RATE_COLUMN = {
    "2021": "2021.12 영업이익률",
    "2022": "2022.12 영업이익률",
    "2023": "2023.12 영업이익률",
    "2024": "2024.12 영업이익률",
}

YEAR_TO_EPS_COLUMN = {
    "2021": "2021.12 EPS(원)",
    "2022": "2022.12 EPS(원)",
    "2023": "2023.12 EPS(원)",
    "2024": "2024.12 EPS(원)",
}

YEAR_TO_PER_COLUMN = {
    "2021": "2021.12 PER(배)",
    "2022": "2022.12 PER(배)",
    "2023": "2023.12 PER(배)",
    "2024": "2024.12 PER(배)",
}

YEAR_TO_PBR_COLUMN = {
    "2021": "2021.12 PBR(배)",
    "2022": "2022.12 PBR(배)",
    "2023": "2023.12 PBR(배)",
    "2024": "2024.12 PBR(배)",
}

YEAR_TO_DIVIDEND_YIELD_COLUMN = {
    "2021": "2021.12 시가배당률(%)",
    "2022": "2022.12 시가배당률(%)",
    "2023": "2023.12 시가배당률(%)",
    "2024": "2024.12 시가배당률(%)",
}

# ------------------------------------
# 분기별 데이터 로드 함수
# ------------------------------------
def load_quarterly_data_from_sqlite():
    """
    SQLite DB에서 quarterly_data 테이블의 데이터를 읽어오고,
    분기 컬럼(2023.Q1, 2023.Q2, ...)에 대해 전처리를 수행.
    ※ PerformanceWarning 방지를 위해, 새 컬럼은 임시 dict에 모아서 한 번에 붙임.
    """
    conn = sqlite3.connect(SQLITE_DB_PATH)
    df_local = pd.read_sql(f"SELECT * FROM {QUARTERLY_TABLE_NAME}", conn)
    conn.close()

    # NaN -> "N/A" 처리
    df_local = df_local.fillna("N/A")

    # 시가총액 -> 숫자 변환 (있으면)
    if "시가총액" in df_local.columns:
        df_local["시가총액(숫자형)"] = df_local["시가총액"].apply(convert_marketcap)

    quarters = ["2023.Q1", "2023.Q2", "2024.Q3", "2024.Q4", "2024.Q5", "2024.Q6"]

    # 새로 만들어야 할 컬럼(= 아직 존재 안 하는 컬럼)을 임시로 담을 딕셔너리
    # key: 컬럼명, value: [값(스칼라 또는 시리즈)] 또는 0, np.nan ...
    columns_to_add = {}

    for q in quarters:
        # 1) 매출액
        revenue_col = f"{q} 매출액"
        if revenue_col in df_local.columns:
            df_local[revenue_col] = df_local[revenue_col].apply(convert_revenue)
        else:
            columns_to_add[revenue_col] = 0  # 일괄 삽입 예정

        # 2) 영업이익
        op_col = f"{q} 영업이익"
        if op_col in df_local.columns:
            df_local[op_col] = df_local[op_col].apply(convert_operating_income)
        else:
            columns_to_add[op_col] = 0

        # 3) 순이익률
        net_col = f"{q} 순이익률"
        if net_col in df_local.columns:
            df_local[net_col] = df_local[net_col].apply(convert_net_income_rate)
        else:
            columns_to_add[net_col] = 0

        # 4) 영업이익률
        op_rate_col = f"{q} 영업이익률"
        if op_rate_col in df_local.columns:
            df_local[op_rate_col] = df_local[op_rate_col].apply(convert_operating_income_rate)
        else:
            columns_to_add[op_rate_col] = 0

        # 5) EPS(원)
        eps_col = f"{q} EPS(원)"
        if eps_col in df_local.columns:
            df_local[eps_col] = df_local[eps_col].apply(convert_eps)
        else:
            columns_to_add[eps_col] = 0

        # 6) PER(배)
        per_col = f"{q} PER(배)"
        if per_col in df_local.columns:
            df_local[per_col] = df_local[per_col].apply(convert_per)
        else:
            columns_to_add[per_col] = 0

        # 7) PBR(배)
        pbr_col = f"{q} PBR(배)"
        if pbr_col in df_local.columns:
            df_local[pbr_col] = df_local[pbr_col].apply(convert_pbr)
        else:
            columns_to_add[pbr_col] = 0

        # 8) 시가배당률(%)
        div_yield_col = f"{q} 시가배당률(%)"
        if div_yield_col in df_local.columns:
            df_local[div_yield_col] = df_local[div_yield_col].apply(convert_dividend_yield)
        else:
            columns_to_add[div_yield_col] = np.nan

        # 9) ROE(지배주주)
        roe_col = f"{q} ROE(지배주주)"
        if roe_col in df_local.columns:
            df_local[roe_col] = df_local[roe_col].apply(convert_roe)
        else:
            columns_to_add[roe_col] = 0

    # ─────────────────────────────────────────────────
    # 새로 추가해야 할 컬럼들(columns_to_add)을 한 번에 df_local에 합친다.
    # ─────────────────────────────────────────────────
    if columns_to_add:
        # columns_to_add는 {colName: 0 or np.nan} 꼴이므로, DataFrame으로 변환
        new_cols_df = pd.DataFrame(columns_to_add, index=df_local.index)
        # df_local과 병합
        df_local = pd.concat([df_local, new_cols_df], axis=1)

    return df_local



# ----------------------------------------
# FastAPI 이벤트: 앱 시작 시점
# ----------------------------------------
@app.on_event("startup")
async def startup_event():
    global annual_df, quarterly_df, dart_df
    
    logger.info("SQLite에서 annual_data 테이블을 불러옵니다...")
    annual_df = load_annual_data_from_sqlite()
    logger.info(f"annual_data 로드: {len(annual_df)} 건")

    logger.info("SQLite에서 quarterly_data 테이블을 불러옵니다...")
    quarterly_df = load_quarterly_data_from_sqlite()
    logger.info(f"quarterly_data 로드: {len(quarterly_df)} 건")

    logger.info("SQLite에서 stock_data 테이블을 불러옵니다...")
    dart_df = load_dart_data_from_sqlite()
    logger.info(f"dart_df 로드: {len(dart_df)} 건")


    

# ----------------------------------------
# 기타 유틸 함수
# ----------------------------------------
def sanitize_dataframe(df_in):
    """NaN, Infinity 값을 처리한 데이터프레임 반환"""
    return df_in.replace([np.inf, -np.inf], np.nan).fillna(0)

def sanitize_value(value):
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return 0
    return value

def calculate_operating_income_rate(row):
    try:
        if row["매출액"] == 0:
            return 0.0
        return round((row["영업이익"] / row["매출액"]) * 100, 1)
    except Exception as e:
        logger.error(f"영업이익률 계산 중 오류 발생: {e}")
        return 0.0

def calculate_debt_ratio(row):
    try:
        if row["자본총계"] == 0:
            return 0.0
        return round((row["부채총계"] / row["자본총계"]) * 100, 1)
    except Exception as e:
        logger.error(f"부채비율 계산 중 오류 발생: {e}")
        return 0.0

# ----------------------------------------
@app.get("/")
def read_root():
    return {"message": "Welcome to my FastAPI server!"}
# ----------------------------------------

# ----------------------------------------
# 2) 각종 API 라우터 (기존 로직 동일)
# ----------------------------------------
@app.get("/data")
def get_stock_data(query: str = Query(..., description="종목 코드 또는 종목명")):
    global df
    try:
        query = query.strip().lower()
        
        df["종목코드"] = df["종목코드"].astype(str).str.strip().str.zfill(6)
        df["종목명_lower"] = df["종목명"].str.lower().str.strip()

        filtered_df = df[
            (df["종목코드"] == query) | (df["종목명_lower"].str.contains(query))
        ]

        if filtered_df.empty:
            return {"error": "검색 결과가 없습니다.", "stocks": []}
        
        filtered_df = sanitize_dataframe(filtered_df)
        data = filtered_df.to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류 발생: {str(e)}", "stocks": []}

@app.get("/top-marketcap")
def get_top_marketcap():
    global df
    try:
        sorted_df = df.sort_values(by="시가총액(숫자형)", ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["시가총액"] = sorted_df["시가총액"].apply(lambda x: f"{x}억")

        data = sorted_df[["순위", "종목명", "시가총액"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": str(e)}

@app.get("/top-revenue")
def get_top_revenue(year: str = Query(..., description="년도 (예: 2021, 2022, 2023, 2024)")):
    global df
    try:
        revenue_column = YEAR_TO_REVENUE_COLUMN.get(year)
        if not revenue_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if revenue_column not in df.columns:
            return {"error": f"{revenue_column} 데이터가 없습니다."}

        sorted_df = df.sort_values(by=revenue_column, ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["매출액"] = sorted_df[revenue_column].apply(format_revenue)

        data = sorted_df[["순위", "종목명", "매출액"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

def format_revenue(value):
    try:
        value = float(value)
        if value >= 10**4:  
            trillion = int(value // 10**4)
            billion = int(value % 10**4)
            if billion == 0:
                return f"{trillion}조"
            return f"{trillion}조 {billion}억"
        elif value >= 1:
            billion = int(value)
            return f"{billion}억"
        else:
            return f"{int(value * 10**8):,}원"
    except (ValueError, TypeError):
        return "N/A"

@app.get("/top-operating-income")
def get_top_operating_income(year: str = Query(..., description="년도 (예: 2021, 2022, 2023, 2024)")):
    global df
    try:
        operating_income_column = YEAR_TO_OPERATING_INCOME_COLUMN.get(year)
        if not operating_income_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if operating_income_column not in df.columns:
            return {"error": f"{operating_income_column} 데이터가 없습니다."}

        sorted_df = df.sort_values(by=operating_income_column, ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["영업이익"] = sorted_df[operating_income_column].apply(format_operating_income)

        data = sorted_df[["순위", "종목명", "영업이익"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

def format_operating_income(value):
    try:
        value = float(value)
        if value >= 10**4:
            trillion = int(value // 10**4)
            billion = int(value % 10**4)
            if billion == 0:
                return f"{trillion}조"
            return f"{trillion}조 {billion}억"
        elif value >= 1:
            billion = int(value)
            return f"{billion}억"
        else:
            return f"{int(value * 10**8):,}원"
    except (ValueError, TypeError):
        return "N/A"

@app.get("/top-net-income")
def get_top_net_income(year: str = Query(..., description="년도 (예: 2021, 2022, 2023, 2024)")):
    global df
    try:
        net_income_rate_column = YEAR_TO_NET_INCOME_RATE_COLUMN.get(year)
        if not net_income_rate_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if net_income_rate_column not in df.columns:
            return {"error": f"{net_income_rate_column} 데이터가 없습니다."}

        sorted_df = df.sort_values(by=net_income_rate_column, ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["순이익률"] = sorted_df[net_income_rate_column].apply(format_net_income_rate)

        data = sorted_df[["순위", "종목명", "순이익률"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

def format_net_income_rate(value):
    try:
        value = float(value)
        return f"{value:.2f}%"
    except (ValueError, TypeError):
        return "N/A"

@app.get("/top-operating-income-rate")
def get_top_operating_income_rate(year: str = Query(..., description="년도 (예: 2021, 2022, 2023, 2024)")):
    global df
    try:
        operating_income_rate_column = YEAR_TO_OPERATING_INCOME_RATE_COLUMN.get(year)
        if not operating_income_rate_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if operating_income_rate_column not in df.columns:
            return {"error": f"{operating_income_rate_column} 데이터가 없습니다."}

        sorted_df = df.sort_values(by=operating_income_rate_column, ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["영업이익률"] = sorted_df[operating_income_rate_column].apply(format_operating_income_rate)

        data = sorted_df[["순위", "종목명", "영업이익률"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

def format_operating_income_rate(value):
    try:
        value = float(value)
        return f"{value:.2f}%"
    except (ValueError, TypeError):
        return "N/A"

@app.get("/top-eps")
def get_top_eps(year: str = Query(..., description="년도 (예: 2021, 2022, 2023, 2024)")):
    global df
    try:
        eps_column = YEAR_TO_EPS_COLUMN.get(year)
        if not eps_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if eps_column not in df.columns:
            return {"error": f"{eps_column} 데이터가 없습니다."}

        sorted_df = df.sort_values(by=eps_column, ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["EPS"] = sorted_df[eps_column].apply(format_eps)

        data = sorted_df[["순위", "종목명", "EPS"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

def format_eps(value):
    try:
        value = float(value)
        return f"{int(value):,}원"
    except (ValueError, TypeError):
        return "N/A"

@app.get("/top-per")
def get_top_per(year: str = Query(..., description="년도 (예: 2021, 2022, 2023, 2024)")):
    global df
    try:
        per_column = YEAR_TO_PER_COLUMN.get(year)
        if not per_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if per_column not in df.columns:
            return {"error": f"{per_column} 데이터가 없습니다."}

        df_filtered = df.dropna(subset=[per_column])
        if df_filtered.empty:
            return {"error": "유효한 PER 데이터가 없습니다.", "stocks": []}

        sorted_df = df_filtered.sort_values(by=per_column, ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["PER"] = sorted_df[per_column].apply(format_per)

        data = sorted_df[["순위", "종목명", "PER"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

@app.get("/bottom-per")
def get_bottom_per(year: str = Query(..., description="년도 (예: 2021, 2022, 2023, 2024)")):
    global df
    try:
        per_column = YEAR_TO_PER_COLUMN.get(year)
        if not per_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if per_column not in df.columns:
            return {"error": f"{per_column} 데이터가 없습니다."}

        df_filtered = df.dropna(subset=[per_column])
        if df_filtered.empty:
            return {"error": "유효한 PER 데이터가 없습니다.", "stocks": []}

        sorted_df = df_filtered.sort_values(by=per_column, ascending=True).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["PER"] = sorted_df[per_column].apply(format_per)

        data = sorted_df[["순위", "종목명", "PER"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

def format_per(value):
    try:
        value = float(value)
        if value == 0:
            return "N/A"
        return f"{value:.1f}배"
    except (ValueError, TypeError):
        return "N/A"

@app.get("/top-pbr")
def get_top_pbr(year: str = Query(..., description="년도 (예: 2021, 2022, 2023, 2024)")):
    global df
    try:
        pbr_column = YEAR_TO_PBR_COLUMN.get(year)
        if not pbr_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if pbr_column not in df.columns:
            return {"error": f"{pbr_column} 데이터가 없습니다."}

        df_filtered = df.dropna(subset=[pbr_column])
        if df_filtered.empty:
            return {"error": "유효한 PBR 데이터가 없습니다.", "stocks": []}

        sorted_df = df_filtered.sort_values(by=pbr_column, ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["PBR"] = sorted_df[pbr_column].apply(format_pbr)

        data = sorted_df[["순위", "종목명", "PBR"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

@app.get("/bottom-pbr")
def get_bottom_pbr(year: str = Query(..., description="년도 (예: 2021, 2022, 2023, 2024)")):
    global df
    try:
        pbr_column = YEAR_TO_PBR_COLUMN.get(year)
        if not pbr_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if pbr_column not in df.columns:
            return {"error": f"{pbr_column} 데이터가 없습니다."}

        positive_pbr_df = df[df[pbr_column] > 0]
        if positive_pbr_df.empty:
            return {"error": "양수 PBR 데이터가 없습니다.", "stocks": []}

        sorted_df = positive_pbr_df.sort_values(by=pbr_column, ascending=True).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["PBR"] = sorted_df[pbr_column].apply(format_pbr)

        data = sorted_df[["순위", "종목명", "PBR"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

def format_pbr(value):
    try:
        value = float(value)
        if np.isnan(value):
            return "N/A"
        return f"{value:.1f}배"
    except (ValueError, TypeError):
        return "N/A"

@app.get("/top-dividend-yield")
def get_top_dividend_yield(year: str = Query(..., description="년도 (예: 2021, 2022, 2023, 2024)")):
    global df
    try:
        dividend_yield_column = YEAR_TO_DIVIDEND_YIELD_COLUMN.get(year)
        if not dividend_yield_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if dividend_yield_column not in df.columns:
            return {"error": f"{dividend_yield_column} 데이터가 없습니다."}

        df_filtered = df.dropna(subset=[dividend_yield_column])
        if df_filtered.empty:
            return {"error": "유효한 시가배당률 데이터가 없습니다.", "stocks": []}

        sorted_df = df_filtered.sort_values(by=dividend_yield_column, ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["시가배당률"] = sorted_df[dividend_yield_column].apply(format_dividend_yield)

        return {"stocks": sorted_df[["순위", "종목명", "시가배당률"]].to_dict(orient="records")}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

def format_dividend_yield(value):
    try:
        value = float(value)
        if np.isnan(value):
            return "N/A"
        return f"{value:.2f}%"
    except (ValueError, TypeError):
        return "N/A"

# ----------------------------------------
# 여기서부터는 "financial_data_sample.xlsx" 사용 로직 제거
# ----------------------------------------
# 기존 엑셀 로드 로직을 제거하고, 데이터베이스에서 로드된 dart_df 사용

@app.get("/financial-annual-sales")
def get_financial_annual_sales(stock_name: str = Query(..., description="기업의 종목명")):
    try:
        if not stock_name.strip():
            return {"error": "종목명이 비어 있습니다.", "annual_sales": []}

        stock_name = stock_name.strip().lower()
        filtered_df = dart_df[dart_df["종목명"].str.lower() == stock_name]

        if filtered_df.empty:
            return {"error": f"종목명 '{stock_name}'을 찾을 수 없습니다.", "annual_sales": []}

        annual_sales = filtered_df[["연도", "매출액"]].copy()
        annual_sales["매출액"] = annual_sales["매출액"] / 10**8  # 억 단위
        annual_sales_sorted = annual_sales.sort_values(by="연도").to_dict(orient="records")

        return {"annual_sales": annual_sales_sorted}
    except Exception as e:
        logger.error(f"연간 매출액 조회 중 오류 발생: {e}")
        return {"annual_sales": [], "error": str(e)}

@app.get("/financial-operating-profit")
def get_financial_operating_profit(stock_name: str = Query(..., description="기업의 종목명")):
    try:
        filtered_df = dart_df[dart_df["종목명"].str.lower() == stock_name.strip().lower()]
        if filtered_df.empty:
            return {"error": "해당 종목명을 찾을 수 없습니다.", "operating_profit": []}

        operating_profit = filtered_df[["연도", "영업이익"]].copy()
        operating_profit["영업이익"] = operating_profit["영업이익"] / 10**8
        operating_profit_sorted = operating_profit.sort_values(by="연도").to_dict(orient="records")
        
        return {"operating_profit": operating_profit_sorted}
    except Exception as e:
        logger.error(f"영업이익 조회 중 오류 발생: {e}")
        return {"operating_profit": [], "error": str(e)}

@app.get("/financial-net-income")
def get_financial_net_income(stock_name: str = Query(..., description="기업의 종목명")):
    try:
        if not stock_name.strip():
            return {"error": "종목명이 비어 있습니다.", "순이익": []}

        stock_name = stock_name.strip().lower()
        filtered_df = dart_df[dart_df["종목명"].str.lower() == stock_name]
        if filtered_df.empty:
            return {"error": f"종목명 '{stock_name}'을 찾을 수 없습니다.", "순이익": []}

        net_income = filtered_df[["연도", "당기순이익"]].copy()
        net_income["순이익"] = net_income["당기순이익"] / 10**8
        net_income_sorted = net_income.sort_values(by="연도").to_dict(orient="records")

        return {"net_income": net_income_sorted}
    except Exception as e:
        logger.error(f"순이익 조회 중 오류 발생: {e}")
        return {"net_income": [], "error": str(e)}

@app.get("/financial-operating-income-rate")
def get_financial_operating_income_rate(
    stock_name: str = Query(..., description="기업의 종목명")
):
    try:
        if not stock_name.strip():
            return {"error": "종목명이 비어 있습니다.", "operating_income_rate": []}

        stock_name = stock_name.strip().lower()
        filtered_df = dart_df[dart_df["종목명"].str.lower() == stock_name]
        if filtered_df.empty:
            return {"error": f"종목명 '{stock_name}'을 찾을 수 없습니다.", "operating_income_rate": []}

        operating_income_rate = filtered_df[["연도", "영업이익률"]].copy()
        operating_income_rate_sorted = operating_income_rate.sort_values(by="연도").to_dict(orient="records")

        return {"operating_income_rate": operating_income_rate_sorted}

    except Exception as e:
        logger.error(f"영업이익률 조회 중 오류 발생: {e}")
        return {"operating_income_rate": [], "error": str(e)}

@app.get("/financial-debt-ratio")
def get_financial_debt_ratio(stock_name: str = Query(..., description="기업의 종목명")):
    try:
        if not stock_name.strip():
            return {"error": "종목명이 비어 있습니다.", "debt_ratio": []}

        stock_name = stock_name.strip().lower()
        filtered_df = dart_df[dart_df["종목명"].str.lower() == stock_name]
        if filtered_df.empty:
            return {"error": f"종목명 '{stock_name}'을 찾을 수 없습니다.", "debt_ratio": []}

        debt_ratio = filtered_df[["연도", "부채비율"]].copy()
        debt_ratio_sorted = debt_ratio.sort_values(by="연도").to_dict(orient="records")

        return {"debt_ratio": debt_ratio_sorted}
    except Exception as e:
        logger.error(f"부채비율 조회 중 오류 발생: {e}")
        return {"debt_ratio": [], "error": str(e)}


# ----------------------------------------
# (추가) 분기별 재무페이지: /quarterly-financial
# ----------------------------------------
# 분기 -> DB 컬럼 prefix
QUARTER_COLUMN_MAP = {
    "2023-Q3": "2023.Q1",
    "2023-Q4": "2023.Q2",
    "2024-Q1": "2023.Q3",
    "2024-Q2": "2023.Q4",
    "2024-Q3": "2023.Q5",
    "2024-Q4": "2023.Q6",
}

# 지표 -> DB 컬럼 suffix
METRIC_COLUMN_MAP = {
    "매출액": "매출액",
    "영업이익": "영업이익",
    "영업이익률": "영업이익률",
    "순이익률": "순이익률",
    "EPS": "EPS(원)",
    "PER": "PER(배)",
    "PBR": "PBR(배)",
    "ROE": "ROE(지배주주)",   # 만약 DB 컬럼이 있다면 여기에 추가
    "시가배당률": "시가배당률(%)",
    # 필요한 다른 지표들도 필요 시 추가...
}

@app.get("/quarterly-financial")
def get_quarterly_financial(
    quarter: str = Query(..., description="예: 2023-Q3, 2023-Q4, 2024-Q1, 2024-Q2, 2024-Q3, 2024-Q4"),
    metric: str = Query(..., description="예: 매출액, 영업이익, 영업이익률, 순이익률, EPS, PER, PBR, ROE, 시가배당률")
):
    """
    예: /quarterly-financial?quarter=2024-Q1&metric=PER
    """
    global quarterly_df
    try:
        # 1) 분기와 지표 매핑
        quarter_prefix = QUARTER_COLUMN_MAP.get(quarter)
        metric_suffix = METRIC_COLUMN_MAP.get(metric)

        if not quarter_prefix or not metric_suffix:
            return {"error": f"유효하지 않은 quarter={quarter} 또는 metric={metric}", "stocks": []}

        final_column = f"{quarter_prefix} {metric_suffix}"

        if final_column not in quarterly_df.columns:
            return {"error": f"'{final_column}' 컬럼이 존재하지 않습니다.", "stocks": []}

        temp_df = quarterly_df.copy()
        temp_df[final_column] = temp_df[final_column].replace("-", np.nan)
        temp_df[final_column] = pd.to_numeric(temp_df[final_column], errors="coerce")

        # dropna, 정렬
        temp_df = temp_df.dropna(subset=[final_column])
        temp_df = temp_df.sort_values(by=final_column, ascending=False)
        top_100 = temp_df.head(100).reset_index(drop=True)

        column_title_for_front = f"{metric} ({quarter})"
        top_100[column_title_for_front] = top_100[final_column]

        # ───────────────────────────────────────────────
        # 2) 지표별로 후처리(조/억 표기 or %/원/배 등)
        # ───────────────────────────────────────────────

        # (가정) 이미 존재하는 조/억 변환 함수
        # format_revenue(value), format_operating_income(value)
        if metric == "매출액":
            top_100[column_title_for_front] = top_100[column_title_for_front].apply(format_revenue)

        elif metric == "영업이익":
            top_100[column_title_for_front] = top_100[column_title_for_front].apply(format_operating_income)

        elif metric in ["영업이익률", "순이익률", "시가배당률"]:
            # float -> xx.xx%
            def append_percent(x):
                try:
                    if pd.isnull(x):
                        return "N/A"
                    return f"{float(x):.2f}%"
                except (ValueError, TypeError):
                    return "N/A"

            top_100[column_title_for_front] = top_100[column_title_for_front].apply(append_percent)

        elif metric == "EPS":
            # float -> xx.xx원 (소수점 버림 or 표시 여부는 자유)
            def append_won(x):
                try:
                    if pd.isnull(x):
                        return "N/A"
                    # 소수점 이하 없애고 3자리마다 콤마
                    return f"{int(x):,}원"
                except (ValueError, TypeError):
                    return "N/A"

            top_100[column_title_for_front] = top_100[column_title_for_front].apply(append_won)

        elif metric in ["PER", "PBR"]:
            # float -> xx.xx배
            def append_bae(x):
                try:
                    if pd.isnull(x):
                        return "N/A"
                    return f"{float(x):.2f}배"
                except (ValueError, TypeError):
                    return "N/A"

            top_100[column_title_for_front] = top_100[column_title_for_front].apply(append_bae)

        elif metric == "ROE":
            # float -> xx.xx%
            def append_percent2(x):
                try:
                    if pd.isnull(x):
                        return "N/A"
                    return f"{float(x):.2f}%"
                except (ValueError, TypeError):
                    return "N/A"

            top_100[column_title_for_front] = top_100[column_title_for_front].apply(append_percent2)

        # 3) 필요한 컬럼만
        top_100["순위"] = top_100.index + 1
        final_df = top_100[["순위", "종목명", column_title_for_front]].copy()

        return {
            "error": "",
            "stocks": final_df.to_dict(orient="records")
        }

    except Exception as e:
        logger.error(f"분기별 재무제표 API 오류: {e}")
        return {"error": str(e), "stocks": []}






if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))  # Cloudtype에서 제공하는 PORT 환경 변수 사용
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)  # 0.0.0.0으로 설정하여 외부에서 접근 가능




# python -m uvicorn main:app --reload
