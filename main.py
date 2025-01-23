from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import logging
import numpy as np
import sqlite3  
import os

# -----------------------------
# 로깅 설정
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI 앱 초기화
app = FastAPI()

# -----------------------------
# CORS 설정
# -----------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://stockter.netlify.app", "http://localhost:3000"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

###############################
# 1) DB 경로/테이블명 설정
###############################
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQLITE_DB_PATH = os.path.join(BASE_DIR, "my_stock.db")

ANNUAL_TABLE_NAME = "annual_data"
QUARTERLY_TABLE_NAME = "quarterly_data"
DART_TABLE_NAME = "stock_data"  # 기존 stock_data 테이블 (연도별)

###############################
# 2) DB 로드 함수들
###############################

def load_annual_data_from_sqlite():
    """
    annual_data 테이블 데이터를 읽어와서
    엑셀에서 하던 전처리(숫자 변환 등)를 수행한 뒤 반환.
    """
    conn = sqlite3.connect(SQLITE_DB_PATH)
    df_local = pd.read_sql(f"SELECT * FROM {ANNUAL_TABLE_NAME}", conn)
    conn.close()

    # NaN -> "N/A" 처리
    df_local = df_local.fillna("N/A")

    # 시가총액 숫자 변환 예시
    df_local["시가총액(숫자형)"] = df_local["시가총액"].apply(convert_marketcap)

    # 연도별 매출액/영업이익 등 숫자 변환
    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        revenue_column = f"{year} 매출액"
        if revenue_column in df_local.columns:
            df_local[revenue_column] = df_local[revenue_column].apply(convert_revenue)
        else:
            df_local[revenue_column] = 0

    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        operating_income_column = f"{year} 영업이익"
        if operating_income_column in df_local.columns:
            df_local[operating_income_column] = df_local[operating_income_column].apply(convert_operating_income)
        else:
            df_local[operating_income_column] = 0

    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        net_income_rate_column = f"{year} 순이익률"
        if net_income_rate_column in df_local.columns:
            df_local[net_income_rate_column] = df_local[net_income_rate_column].apply(convert_net_income_rate)
        else:
            df_local[net_income_rate_column] = 0

    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        operating_income_rate_column = f"{year} 영업이익률"
        if operating_income_rate_column in df_local.columns:
            df_local[operating_income_rate_column] = df_local[operating_income_rate_column].apply(convert_operating_income_rate)
        else:
            df_local[operating_income_rate_column] = 0

    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        eps_column = f"{year} EPS(원)"
        if eps_column in df_local.columns:
            df_local[eps_column] = df_local[eps_column].apply(convert_eps)
        else:
            df_local[eps_column] = 0

    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        per_column = f"{year} PER(배)"
        if per_column in df_local.columns:
            df_local[per_column] = df_local[per_column].apply(convert_per)
        else:
            df_local[per_column] = 0

    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        pbr_column = f"{year} PBR(배)"
        if pbr_column in df_local.columns:
            df_local[pbr_column] = df_local[pbr_column].apply(convert_pbr)
        else:
            df_local[pbr_column] = 0

    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        dividend_yield_column = f"{year} 시가배당률(%)"
        if dividend_yield_column in df_local.columns:
            df_local[dividend_yield_column] = df_local[dividend_yield_column].apply(convert_dividend_yield)
        else:
            df_local[dividend_yield_column] = np.nan

    for year in ['2021.12', '2022.12', '2023.12', '2024.12']:
        col = f"{year} ROE(지배주주)"
        if col in df_local.columns:
            df_local[col] = df_local[col].apply(convert_roe)
        else:
            df_local[col] = np.nan

    return df_local

def load_dart_data_from_sqlite():
    """
    stock_data 테이블(dart_df)을 읽어와 전처리 후 반환.
    """
    conn = sqlite3.connect(SQLITE_DB_PATH)
    dart_df_local = pd.read_sql(f"SELECT * FROM {DART_TABLE_NAME}", conn)
    conn.close()

    # NaN -> "N/A"
    dart_df_local = dart_df_local.fillna("N/A")

    # 숫자형 변환(매출액, 영업이익, 등등)
    dart_df_local["매출액"] = dart_df_local["매출액"].apply(convert_financial_revenue)
    dart_df_local["영업이익"] = dart_df_local["영업이익"].apply(convert_financial_operating_profit)
    dart_df_local["당기순이익"] = dart_df_local["당기순이익"].apply(convert_financial_net_income)
    dart_df_local["영업이익률"] = dart_df_local.apply(calculate_operating_income_rate, axis=1)
    dart_df_local["부채총계"] = dart_df_local["부채총계"].apply(convert_financial_total_debt)
    dart_df_local["자본총계"] = dart_df_local["자본총계"].apply(convert_financial_total_equity)
    dart_df_local["부채비율"] = dart_df_local.apply(calculate_debt_ratio, axis=1)

    return dart_df_local

# -----------------------------
# 숫자 변환 함수들 (기존 로직 유지)
# -----------------------------
def convert_marketcap(value):
    if isinstance(value, str):
        value = value.replace(",", "").strip()
        if "조" in value:
            parts = value.split("조")
            trillion = float(parts[0]) * 10**12
            if len(parts) > 1 and parts[1]:
                trillion += float(parts[1]) * 10**8
            return trillion
        elif "억" in value:
            return float(value.replace("억", "")) * 10**8
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
        elif value in ["N/A", "-"]:
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
        elif value in ["N/A", "-"]:
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

def convert_roe(value):
    if isinstance(value, str):
        value = value.replace("%", "").replace(",", "").replace("배", "").strip()
        if value in ["N/A", "-", ""]:
            return np.nan
        try:
            return float(value)
        except:
            return np.nan
    try:
        return float(value)
    except:
        return np.nan

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
        if value.upper() == "N/A" or value.strip() == "-":
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
# 전역 annual_df, quarterly_df, dart_df
# ----------------------------------------
annual_df = None
quarterly_df = None
dart_df = None

# ----------------------------------------
# 연도별 컬럼 매핑 (기존 사용 로직)
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

YEAR_TO_ROE_COLUMN = {
    "2021": "2021.12 ROE(지배주주)",
    "2022": "2022.12 ROE(지배주주)",
    "2023": "2023.12 ROE(지배주주)",
    "2024": "2024.12 ROE(지배주주)",
}

# ------------------------------------
# 분기별 데이터 로드 함수
# ------------------------------------
def load_quarterly_data_from_sqlite():
    """
    quarterly_data 테이블에서 분기별 데이터 불러오기.
    '2023.Q1 매출액', '2023.Q2 매출액' 등의 컬럼이 이미 존재한다고 가정.
    """
    conn = sqlite3.connect(SQLITE_DB_PATH)
    df_local = pd.read_sql(f"SELECT * FROM {QUARTERLY_TABLE_NAME}", conn)
    conn.close()

    # NaN -> "N/A"
    df_local = df_local.fillna("N/A")

    # 시가총액(숫자형) 변환
    if "시가총액" in df_local.columns:
        df_local["시가총액(숫자형)"] = df_local["시가총액"].apply(convert_marketcap)

    # 2023.Q1 ~ 2023.Q6에 대해 매출액, 영업이익 등 숫자 변환
    for i in range(1, 7):
        q_prefix = f"2023.Q{i}"
        rev_col = f"{q_prefix} 매출액"
        if rev_col in df_local.columns:
            df_local[rev_col] = df_local[rev_col].apply(convert_revenue)

        op_col = f"{q_prefix} 영업이익"
        if op_col in df_local.columns:
            df_local[op_col] = df_local[op_col].apply(convert_operating_income)

        net_col = f"{q_prefix} 당기순이익"
        if net_col in df_local.columns:
            df_local[net_col] = df_local[net_col].apply(convert_operating_income)  # 당기순이익도 영업이익 변환 방식과 동일

        op_rate_col = f"{q_prefix} 영업이익률"
        if op_rate_col in df_local.columns:
            df_local[op_rate_col] = df_local[op_rate_col].apply(convert_operating_income_rate)

        ni_rate_col = f"{q_prefix} 순이익률"
        if ni_rate_col in df_local.columns:
            df_local[ni_rate_col] = df_local[ni_rate_col].apply(convert_net_income_rate)

        # 부채비율 등 다른 컬럼들도 필요 시 동일하게 처리

    return df_local

# ----------------------------------------
# 앱 시작 시점 이벤트: DB에서 데이터 로드
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
# 유틸 함수들
# ----------------------------------------
def sanitize_dataframe(df_in):
    """NaN, inf 값을 0으로 채우는 예시 함수"""
    return df_in.replace([np.inf, -np.inf], np.nan).fillna(0)

def sanitize_value(value):
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return 0
    return value

def calculate_operating_income_rate(row):
    """(영업이익 / 매출액) * 100"""
    try:
        if row["매출액"] == 0:
            return 0.0
        return round((row["영업이익"] / row["매출액"]) * 100, 1)
    except Exception as e:
        logger.error(f"영업이익률 계산 중 오류 발생: {e}")
        return 0.0

def calculate_debt_ratio(row):
    """(부채총계 / 자본총계) * 100"""
    try:
        if row["자본총계"] == 0:
            return 0.0
        return round((row["부채총계"] / row["자본총계"]) * 100, 1)
    except Exception as e:
        logger.error(f"부채비율 계산 중 오류 발생: {e}")
        return 0.0

# ----------------------------------------
# 기본 라우트: 테스트용
# ----------------------------------------
@app.get("/")
def read_root():
    return {"message": "Welcome to my FastAPI server!"}

# ----------------------------------------
# (A) 연간 재무 API들 (연도별)
# ----------------------------------------
@app.get("/data")
def get_stock_data(query: str = Query(..., description="종목 코드 또는 종목명")):
    """
    연간 데이터(annual_df)에서 종목명/코드 검색
    """
    global annual_df  
    try:
        query = query.strip().lower()
        
        annual_df["종목코드"] = annual_df["종목코드"].astype(str).str.strip().str.zfill(6)
        annual_df["종목명_lower"] = annual_df["종목명"].str.lower().str.strip()

        filtered_df = annual_df[
            (annual_df["종목코드"] == query) | (annual_df["종목명_lower"].str.contains(query))
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
    """
    시가총액(숫자형) 기준 상위 100개
    """
    global annual_df  
    try:
        sorted_df = annual_df.sort_values(by="시가총액(숫자형)", ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["시가총액"] = sorted_df["시가총액"].apply(lambda x: f"{x}억")

        data = sorted_df[["순위", "종목명", "시가총액"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": str(e)}

@app.get("/top-revenue")
def get_top_revenue(year: str = Query(..., description="년도 (예: 2021, 2022, 2023, 2024)")):
    """
    해당 년도의 매출액 상위 100개
    """
    global annual_df
    try:
        revenue_column = YEAR_TO_REVENUE_COLUMN.get(year)
        if not revenue_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if revenue_column not in annual_df.columns:
            return {"error": f"{revenue_column} 데이터가 없습니다."}

        sorted_df = annual_df.sort_values(by=revenue_column, ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["매출액"] = sorted_df[revenue_column].apply(format_revenue)

        data = sorted_df[["순위", "종목명", "매출액"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

def format_revenue(value):
    """
    숫자를 조/억/원 단위로 문자열화
    """
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
def get_top_operating_income(year: str = Query(..., description="년도")):
    """
    해당 년도의 영업이익 상위 100개
    """
    global annual_df
    try:
        operating_income_column = YEAR_TO_OPERATING_INCOME_COLUMN.get(year)
        if not operating_income_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if operating_income_column not in annual_df.columns:
            return {"error": f"{operating_income_column} 데이터가 없습니다."}

        sorted_df = annual_df.sort_values(by=operating_income_column, ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["영업이익"] = sorted_df[operating_income_column].apply(format_operating_income)

        data = sorted_df[["순위", "종목명", "영업이익"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

def format_operating_income(value):
    """
    조/억/원 형식으로 변환
    """
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
def get_top_net_income(year: str = Query(..., description="년도")):
    """
    해당 년도의 순이익률 상위 100개
    """
    global annual_df
    try:
        net_income_rate_column = YEAR_TO_NET_INCOME_RATE_COLUMN.get(year)
        if not net_income_rate_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if net_income_rate_column not in annual_df.columns:
            return {"error": f"{net_income_rate_column} 데이터가 없습니다."}

        sorted_df = annual_df.sort_values(by=net_income_rate_column, ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["순이익률"] = sorted_df[net_income_rate_column].apply(format_net_income_rate)

        data = sorted_df[["순위", "종목명", "순이익률"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

def format_net_income_rate(value):
    """
    순이익률 -> 소수점 2자리+%
    """
    try:
        value = float(value)
        return f"{value:.2f}%"
    except (ValueError, TypeError):
        return "N/A"

@app.get("/top-operating-income-rate")
def get_top_operating_income_rate(year: str = Query(..., description="년도")):
    """
    해당 년도의 영업이익률 상위 100개
    """
    global annual_df
    try:
        operating_income_rate_column = YEAR_TO_OPERATING_INCOME_RATE_COLUMN.get(year)
        if not operating_income_rate_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if operating_income_rate_column not in annual_df.columns:
            return {"error": f"{operating_income_rate_column} 데이터가 없습니다."}

        sorted_df = annual_df.sort_values(by=operating_income_rate_column, ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["영업이익률"] = sorted_df[operating_income_rate_column].apply(format_operating_income_rate)

        data = sorted_df[["순위", "종목명", "영업이익률"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

def format_operating_income_rate(value):
    """
    영업이익률 -> 소수점 2자리+%
    """
    try:
        value = float(value)
        return f"{value:.2f}%"
    except (ValueError, TypeError):
        return "N/A"

@app.get("/top-roe")
def get_top_roe(year: str = Query(..., description="년도")):
    """
    ROE(지배주주) 기준 상위 100개
    """
    global annual_df
    try:
        roe_col = YEAR_TO_ROE_COLUMN.get(year)
        if not roe_col:
            return {"error": f"지원되지 않는 연도입니다: {year}.", "stocks": []}

        if roe_col not in annual_df.columns:
            return {"error": f"'{roe_col}' 데이터가 없습니다.", "stocks": []}

        sorted_df = annual_df.sort_values(by=roe_col, ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1

        def format_roe_percent(v):
            try:
                x = float(v)
                return f"{x:.2f}%"
            except:
                return "N/A"

        sorted_df["ROE"] = sorted_df[roe_col].apply(format_roe_percent)
        data = sorted_df[["순위", "종목명", "ROE"]].to_dict(orient="records")
        return {"stocks": data}

    except Exception as e:
        return {"error": f"서버 오류: {str(e)}", "stocks": []}

@app.get("/top-eps")
def get_top_eps(year: str = Query(..., description="년도")):
    """
    EPS(원) 기준 상위 100개
    """
    global annual_df
    try:
        eps_column = YEAR_TO_EPS_COLUMN.get(year)
        if not eps_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if eps_column not in annual_df.columns:
            return {"error": f"{eps_column} 데이터가 없습니다."}

        sorted_df = annual_df.sort_values(by=eps_column, ascending=False).head(100)
        sorted_df = sorted_df.reset_index(drop=True)
        sorted_df["순위"] = sorted_df.index + 1
        sorted_df["EPS"] = sorted_df[eps_column].apply(format_eps)

        data = sorted_df[["순위", "종목명", "EPS"]].to_dict(orient="records")
        return {"stocks": data}
    except Exception as e:
        return {"error": f"서버 오류: {str(e)}"}

def format_eps(value):
    """
    EPS -> 1,000원 형식
    """
    try:
        value = float(value)
        return f"{int(value):,}원"
    except (ValueError, TypeError):
        return "N/A"

@app.get("/top-per")
def get_top_per(year: str = Query(..., description="년도")):
    """
    PER(배) 상위 100 (정확히는 내림차순 정렬)
    """
    global annual_df
    try:
        per_column = YEAR_TO_PER_COLUMN.get(year)
        if not per_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if per_column not in annual_df.columns:
            return {"error": f"{per_column} 데이터가 없습니다."}

        df_filtered = annual_df.dropna(subset=[per_column])
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
def get_bottom_per(year: str = Query(..., description="년도")):
    """
    PER(배) 하위 100 (오름차순 정렬)
    """
    global annual_df
    try:
        per_column = YEAR_TO_PER_COLUMN.get(year)
        if not per_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if per_column not in annual_df.columns:
            return {"error": f"{per_column} 데이터가 없습니다."}

        df_filtered = annual_df.dropna(subset=[per_column])
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
    """
    PER -> 소수점 1자리+배
    """
    try:
        value = float(value)
        if value == 0:
            return "N/A"
        return f"{value:.1f}배"
    except (ValueError, TypeError):
        return "N/A"

@app.get("/top-pbr")
def get_top_pbr(year: str = Query(..., description="년도")):
    """
    PBR(배) 상위 100 (내림차순 정렬)
    """
    global annual_df
    try:
        pbr_column = YEAR_TO_PBR_COLUMN.get(year)
        if not pbr_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if pbr_column not in annual_df.columns:
            return {"error": f"{pbr_column} 데이터가 없습니다."}

        df_filtered = annual_df.dropna(subset=[pbr_column])
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
def get_bottom_pbr(year: str = Query(..., description="년도")):
    """
    PBR(배) 하위 100 (오름차순 정렬),
    0 이하(음수 PBR)는 제외 예시
    """
    global annual_df
    try:
        pbr_column = YEAR_TO_PBR_COLUMN.get(year)
        if not pbr_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if pbr_column not in annual_df.columns:
            return {"error": f"{pbr_column} 데이터가 없습니다."}

        positive_pbr_df = annual_df[annual_df[pbr_column] > 0]
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
    """
    PBR -> 소수점 1자리+배
    """
    try:
        value = float(value)
        if np.isnan(value):
            return "N/A"
        return f"{value:.1f}배"
    except (ValueError, TypeError):
        return "N/A"

@app.get("/top-dividend-yield")
def get_top_dividend_yield(year: str = Query(..., description="년도")):
    """
    시가배당률(%) 상위 100
    """
    global annual_df
    try:
        dividend_yield_column = YEAR_TO_DIVIDEND_YIELD_COLUMN.get(year)
        if not dividend_yield_column:
            return {"error": f"지원되지 않는 연도입니다: {year}."}

        if dividend_yield_column not in annual_df.columns:
            return {"error": f"{dividend_yield_column} 데이터가 없습니다."}

        df_filtered = annual_df.dropna(subset=[dividend_yield_column])
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
    """
    시가배당률 -> 소수점 2자리+%
    """
    try:
        value = float(value)
        if np.isnan(value):
            return "N/A"
        return f"{value:.2f}%"
    except (ValueError, TypeError):
        return "N/A"

# ----------------------------------------
# (B) DART DF 활용: 연간 재무제표
# ----------------------------------------
@app.get("/financial-annual-sales")
def get_financial_annual_sales(stock_name: str = Query(...)):
    """
    연간 매출액 (dart_df에서 가져옴)
    """
    try:
        if not stock_name.strip():
            return {"error": "종목명이 비어 있습니다.", "annual_sales": []}

        stock_name = stock_name.strip().lower()
        filtered_df = dart_df[dart_df["종목명"].str.lower() == stock_name]

        if filtered_df.empty:
            return {"error": f"종목명 '{stock_name}'을 찾을 수 없습니다.", "annual_sales": []}

        annual_sales = filtered_df[["연도", "매출액"]].copy()
        annual_sales["매출액"] = annual_sales["매출액"] / 10**8  # 억 단위로 변환
        annual_sales_sorted = annual_sales.sort_values(by="연도").to_dict(orient="records")

        return {"annual_sales": annual_sales_sorted}
    except Exception as e:
        logger.error(f"연간 매출액 조회 중 오류 발생: {e}")
        return {"annual_sales": [], "error": str(e)}

@app.get("/financial-operating-profit")
def get_financial_operating_profit(stock_name: str = Query(...)):
    """
    연간 영업이익
    """
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
def get_financial_net_income(stock_name: str = Query(...)):
    """
    연간 당기순이익
    """
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
        logger.error(f"순이익 조회 중 오류: {e}")
        return {"net_income": [], "error": str(e)}

@app.get("/financial-operating-income-rate")
def get_financial_operating_income_rate(stock_name: str = Query(...)):
    """
    연간 영업이익률
    """
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
def get_financial_debt_ratio(stock_name: str = Query(...)):
    """
    연간 부채비율
    """
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
# (D) 분기별 재무 데이터 (quarterly_df)
# ----------------------------------------

# 분기(i=1~6)를 새로 매핑할 딕셔너리(예시)
quarter_mapping = {
    1: "2023-Q3",  # i=1 -> "2023-Q3"
    2: "2023-Q4",
    3: "2024-Q1",
    4: "2024-Q2",
    5: "2024-Q3",
    6: "2024-Q4",
}

@app.get("/financial-quarterly-sales")
def get_financial_quarterly_sales(stock_name: str = Query(...)):
    """
    분기별 매출액: DB 내 "2023.Q1 매출액" ~ "2023.Q6 매출액"을 읽어
    분기 이름을 "2023-Q3" 등으로 변환하여 반환.
    """
    try:
        sname = stock_name.strip().lower()
        filtered_df = quarterly_df[quarterly_df["종목명"].str.lower() == sname]
        if filtered_df.empty:
            return {"error": f"'{stock_name}' 종목 없음", "quarterly_sales": []}

        row = filtered_df.iloc[0]
        results = []
        for i in range(1, 7):
            col_name = f"2023.Q{i} 매출액"
            if col_name not in row:
                continue

            new_quarter_name = quarter_mapping.get(i, f"2023.Q{i}")
            raw_val = row[col_name]
            # DB에 이미 '억' 단위라면, 1e8로 나누지 않고 그대로 사용
            value_in_억 = raw_val if raw_val else 0

            results.append({
                "분기": new_quarter_name,
                "매출액": value_in_억
            })

        return {"quarterly_sales": results}

    except Exception as e:
        logger.error(f"분기별 매출액 조회 중 오류: {e}")
        return {"quarterly_sales": [], "error": str(e)}


@app.get("/financial-quarterly-operating-profit")
def get_financial_quarterly_operating_profit(stock_name: str = Query(...)):
    """
    분기별 영업이익: "2023.Q1 영업이익" ~ "2023.Q6 영업이익"
    """
    try:
        sname = stock_name.strip().lower()
        filtered_df = quarterly_df[quarterly_df["종목명"].str.lower() == sname]
        if filtered_df.empty:
            return {"error": f"'{stock_name}' 종목 없음", "quarterly_operating_profit": []}

        row = filtered_df.iloc[0]
        results = []
        for i in range(1, 7):
            col_name = f"2023.Q{i} 영업이익"
            if col_name not in row:
                continue

            raw_val = row[col_name]
            # DB에 이미 '억' 단위라면, 1e8로 나누지 않고 그대로 사용
            value_in_억 = raw_val if raw_val else 0

            new_quarter_name = quarter_mapping.get(i, f"2023.Q{i}")

            results.append({
                "분기": new_quarter_name,
                "영업이익": value_in_억
            })

        return {"quarterly_operating_profit": results}

    except Exception as e:
        logger.error(f"분기별 영업이익 조회 중 오류: {e}")
        return {"quarterly_operating_profit": [], "error": str(e)}


@app.get("/financial-quarterly-net-income")
def get_financial_quarterly_net_income(stock_name: str = Query(...)):
    """
    분기별 당기순이익: "2023.Q1 당기순이익" ~ "2023.Q6 당기순이익"
    """
    try:
        sname = stock_name.strip().lower()
        filtered_df = quarterly_df[quarterly_df["종목명"].str.lower() == sname]
        if filtered_df.empty:
            return {"error": f"'{stock_name}' 종목 없음", "quarterly_net_income": []}

        row = filtered_df.iloc[0]
        results = []
        for i in range(1, 7):
            col_name = f"2023.Q{i} 당기순이익"
            if col_name not in row:
                continue

            raw_val = row[col_name]
            # DB에 이미 '억' 단위라면, 1e8로 나누지 않고 그대로 사용
            value_in_억 = raw_val if raw_val else 0

            new_quarter_name = quarter_mapping.get(i, f"2023.Q{i}")

            results.append({
                "분기": new_quarter_name,
                "순이익": value_in_억
            })

        return {"quarterly_net_income": results}

    except Exception as e:
        logger.error(f"분기별 순이익 조회 중 오류: {e}")
        return {"quarterly_net_income": [], "error": str(e)}



@app.get("/financial-quarterly-operating-income-rate")
def get_financial_quarterly_operating_income_rate(stock_name: str = Query(...)):
    """
    분기별 영업이익률: "2023.Q1 영업이익률" ~ "2023.Q6 영업이익률"
    """
    try:
        sname = stock_name.strip().lower()
        filtered_df = quarterly_df[quarterly_df["종목명"].str.lower() == sname]
        if filtered_df.empty:
            return {"error": f"'{stock_name}' 종목 없음", "quarterly_operating_income_rate": []}

        row = filtered_df.iloc[0]
        results = []
        for i in range(1, 7):
            col_name = f"2023.Q{i} 영업이익률"
            if col_name not in row:
                continue

            raw_val = row[col_name] or 0
            new_quarter_name = quarter_mapping.get(i, f"2023.Q{i}")

            results.append({
                "분기": new_quarter_name,
                "영업이익률": raw_val
            })

        return {"quarterly_operating_income_rate": results}

    except Exception as e:
        logger.error(f"분기별 영업이익률 조회 중 오류: {e}")
        return {"quarterly_operating_income_rate": [], "error": str(e)}


@app.get("/financial-quarterly-debt-ratio")
def get_financial_quarterly_debt_ratio(stock_name: str = Query(...)):
    """
    분기별 부채비율: "2023.Q1 부채비율" ~ "2023.Q6 부채비율"
    """
    try:
        sname = stock_name.strip().lower()
        filtered_df = quarterly_df[quarterly_df["종목명"].str.lower() == sname]
        if filtered_df.empty:
            return {"error": f"'{stock_name}' 종목 없음", "quarterly_debt_ratio": []}

        row = filtered_df.iloc[0]
        results = []
        for i in range(1, 7):
            col_name = f"2023.Q{i} 부채비율"
            if col_name not in row:
                continue

            raw_val = row[col_name] or 0
            new_quarter_name = quarter_mapping.get(i, f"2023.Q{i}")

            results.append({
                "분기": new_quarter_name,
                "부채비율": raw_val
            })

        return {"quarterly_debt_ratio": results}

    except Exception as e:
        logger.error(f"분기별 부채비율 조회 중 오류: {e}")
        return {"quarterly_debt_ratio": [], "error": str(e)}


# ----------------------------------------
# (E) 분기별 특정 지표 정렬 조회 예시
# ----------------------------------------
METRIC_COLUMN_MAP = {
    "매출액": "매출액",
    "영업이익": "영업이익",
    "영업이익률": "영업이익률",
    "순이익률": "순이익률",
    "EPS": "EPS(원)",
    "PER": "PER(배)",
    "PBR": "PBR(배)",
    "ROE": "ROE(지배주주)",
    "시가배당률": "시가배당률(%)",
    # 필요한 지표가 있다면 여기에 추가
}

# 여기에선 예시로 '2023-Q3' -> '2023.Q1' 식의 역매핑이 필요할 수 있으니
# QUARTER_COLUMN_MAP 등을 만들 수도 있음 (생략)

@app.get("/quarterly-financial")
def get_quarterly_financial(
    quarter: str = Query(..., description="예: 2023-Q3, 2023-Q4, 2024-Q1, ..."),
    metric: str = Query(..., description="매출액, 영업이익, 영업이익률, etc."),
    order: str = Query("top", description="정렬 순서: 'top' (오름차순) or 'bottom' (내림차순)")
):
    """
    예) /quarterly-financial?quarter=2024-Q1&metric=PER&order=top
    분기별 특정 지표 순위 내림차순/오름차순 정렬
    """
    global quarterly_df
    try:
        # (예시) quarter -> "2023.Q1" 등으로 매핑 필요 시 작성
        # 여기서는 QUARTER_COLUMN_MAP이 있다고 가정
        # quarter_prefix = QUARTER_COLUMN_MAP.get(quarter)  # 생략

        metric_suffix = METRIC_COLUMN_MAP.get(metric)
        if not metric_suffix:
            return {"error": f"유효하지 않은 metric={metric}", "stocks": []}

        # 예) final_column = f"{quarter_prefix} {metric_suffix}"
        # 실제로는 2023-Q3 -> 2023.Q1이라는 매핑이 필요할 수도 있음
        # 여기서는 단순 예시이므로 직접 구성 가정
        final_column = "2023.Q1 " + metric_suffix  # 예시...

        if final_column not in quarterly_df.columns:
            return {"error": f"'{final_column}' 컬럼이 존재하지 않습니다.", "stocks": []}

        temp_df = quarterly_df.copy()
        temp_df[final_column] = pd.to_numeric(temp_df[final_column], errors="coerce")
        temp_df = temp_df.dropna(subset=[final_column])

        # 정렬
        if metric in ["PER", "PBR"]:
            ascending = True if order == "top" else False
            temp_df = temp_df.sort_values(by=final_column, ascending=ascending)
        else:
            temp_df = temp_df.sort_values(by=final_column, ascending=False)

        top_100 = temp_df.head(100).reset_index(drop=True)
        top_100["순위"] = top_100.index + 1

        # 후속 처리(포맷팅) - 생략 (매출액이면 조/억 변환 등)

        return {"error": "", "stocks": top_100[["순위", "종목명", final_column]].to_dict(orient="records")}

    except Exception as e:
        logger.error(f"분기별 재무제표 API 오류: {e}")
        return {"error": str(e), "stocks": []}


# ----------------------------------------
# main (로컬 실행 시)
# ----------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))  # Cloudtype 등 환경에서 PORT 사용
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)  # 외부 접속 가능
