from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
# from pykiwoom.kiwoom import Kiwoom
# import time
# import asyncio
import logging
# import pythoncom
import numpy as np
import sqlite3  # ★ 추가
import os

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # 실제 배포 시 특정 도메인으로 제한 권장
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
TABLE_NAME = "krx_stock_data"

def load_data_from_sqlite():
    """
    SQLite DB에서 데이터를 읽어오고,
    기존 엑셀에서 하던 전처리를 동일하게 수행한 뒤 df를 반환.
    """
    conn = sqlite3.connect(SQLITE_DB_PATH)
    df_local = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
    conn.close()

    # 엑셀 읽었을 때처럼 NaN -> "N/A" 처리
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


# (★ 기존에 엑셀 로드하던 코드는 제거/주석 처리했습니다.)
# EXCEL_FILE_PATH = r"C:\Users\redwh\Desktop\개발\stock\krx_stock_data_sorted.xlsx"
# df = pd.read_excel(EXCEL_FILE_PATH)
# df = df.fillna("N/A")

# -----------------------------
# 숫자 변환 함수들 (기존 코드 그대로)
# -----------------------------
def convert_marketcap(value):
    if isinstance(value, str):
        if "조" in value:
            parts = value.split("조")
            trillion = float(parts[0]) * 10**12
            if len(parts) > 1 and parts[1]:
                trillion += float(parts[1].strip()) * 10**8
            return trillion
        elif "억" in value:
            return float(value.replace("억", "").strip()) * 10**8
    return float(value)

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
    return float(value)

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
    return float(value)

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
    return float(value)

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
    return float(value)

def convert_eps(value):
    if isinstance(value, str):
        value = value.replace("원", "").replace(",", "").strip()
        if value in ["N/A", "-"]:
            return 0
        try:
            return float(value)
        except ValueError:
            return 0
    return float(value)

def convert_per(value):
    if isinstance(value, str):
        value = value.replace("배", "").replace(",", "").strip()
        if value in ["N/A", "-"]:
            return np.nan
        try:
            return float(value)
        except ValueError:
            return np.nan
    return float(value)

def convert_pbr(value):
    if isinstance(value, str):
        value = value.replace("배", "").replace(",", "").strip()
        if value in ["N/A", "-"]:
            return np.nan
        try:
            return float(value)
        except ValueError:
            return np.nan
    return float(value)

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


# ----------------------------------------
# 전역 df (★ DB에서 읽어온 결과 저장)
# ----------------------------------------
df = None

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


# ----------------------------------------
# 키움 API 클래스화 (기존 코드와 동일)
# ----------------------------------------
# class KiwoomAPI:
#     def __init__(self):
#         self.kiwoom = Kiwoom()
#         self.data_received = False
#         self.raw_data = None
#         self.screen_no_counter = 1000
#         self.kiwoom.OnReceiveTrData = self.on_receive_tr_data
#         logger.info("OnReceiveTrData 이벤트 핸들러 등록 완료")

#     def _get_screen_no(self) -> str:
#         screen_no = str(self.screen_no_counter).zfill(4)
#         self.screen_no_counter += 1
#         if self.screen_no_counter > 9999:
#             self.screen_no_counter = 1000
#         return screen_no
    
#     def on_receive_tr_data(self, screen_no, rqname, trcode, recordname, prev_next):
#         logger.info(f"on_receive_tr_data called with rqname={rqname}, trcode={trcode}, screen_no={screen_no}")
#         if rqname == "주식재무제표요청" and trcode == "OPT10087":
#             try:
#                 self.raw_data = self.kiwoom.GetCommDataEx(trcode, recordname)
#                 logger.info(f"Received raw_data: {self.raw_data}")
#                 self.data_received = True
#             except Exception as e:
#                 logger.error(f"키움 API 데이터 수신 중 오류 발생: {e}")
#                 self.raw_data = None
#                 self.data_received = True

#     def get_revenue_history(self, stock_code: str):
#         logger.info(f"get_revenue_history() called with stock_code={stock_code}")
#         self.data_received = False
#         self.raw_data = None

#         try:
#             logger.info("TR 요청값 설정 시작")
#             self.kiwoom.SetInputValue("종목코드", stock_code)
#             logger.info(f"SetInputValue - 종목코드: {stock_code}")

#             self.kiwoom.SetInputValue("표시구분", "1")  # 연간 매출액 요청
#             logger.info("SetInputValue - 표시구분: 1")
#         except Exception as e:
#             logger.error(f"SetInputValue 오류: {e}")
#             return {}

#         screen_no = self._get_screen_no()
#         try:
#             logger.info("CommRqData 호출 시작")
#             self.kiwoom.CommRqData("주식재무제표요청", "OPT10087", 0, screen_no) 
#             logger.info("CommRqData 호출 완료")
#         except Exception as e:
#             logger.error(f"CommRqData 오류: {e}")
#             return {}

#         logger.info("데이터 수신 대기 시작")
#         for i in range(30):
#             pythoncom.PumpWaitingMessages()
#             if self.data_received:
#                 logger.info("데이터 수신 완료")
#                 break
#             logger.info(f"데이터 수신 대기 중... ({i + 1}/30)")
#             time.sleep(1)

#         if not self.raw_data:
#             logger.error("키움 API 데이터 수신 실패")
#             return {}

#         logger.info(f"키움 API 데이터 수신 성공: {self.raw_data}")
#         logger.info("데이터 변환 없이 원본 데이터 반환")
#         return {"raw_data": self.raw_data}

# # 키움 API 인스턴스
# kiwoom_api = KiwoomAPI()


# # ----------------------------------------
# # FastAPI 이벤트: 앱 시작 시점
# # ----------------------------------------
@app.on_event("startup")
async def startup_event():
    # logger.info("키움 Open API 로그인 중...")
    # kiwoom_api.kiwoom.CommConnect(block=True)
    # while kiwoom_api.kiwoom.GetConnectState() == 0:
    #     logger.info("키움 Open API 로그인 대기 중...")
    #     await asyncio.sleep(1)
    # logger.info("키움 Open API 로그인 완료")

    # (★ 추가) 앱 시작 시점에 DB에서 데이터 불러오기
    global df
    logger.info("SQLite에서 krx_stock_data 테이블을 불러옵니다...")
    df = load_data_from_sqlite()
    logger.info(f"DB로부터 데이터를 성공적으로 로드했습니다. 총 {len(df)}건")


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


# @app.get("/annual-sales")
# def get_annual_sales(stock_code: str):
#     logger.info(f"/annual-sales called with stock_code={stock_code}")
#     try:
#         response = kiwoom_api.get_revenue_history(stock_code)
#         logger.info(f"Received Kiwoom data: {response}")
#         if not response or "raw_data" not in response:
#             logger.error("키움 API에서 데이터를 반환하지 않았습니다.")
#             return {"annual_sales": [], "error": "키움 API 데이터가 없습니다."}

#         raw_data = response["raw_data"]
#         logger.info(f"Final raw_data: {raw_data}")
#         return {"annual_sales": raw_data}
#     except Exception as e:
#         logger.error(f"연간 매출액 조회 중 오류 발생: {e}")
#         return {"annual_sales": [], "error": str(e)}


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
# 여기서부터는 "financial_data_sample.xlsx" 사용 로직 (기존 그대로)
# ----------------------------------------
FINANCIAL_EXCEL_FILE_PATH = r"C:\Users\redwh\Desktop\개발\stock\financial_data_sample.xlsx"
financial_df = pd.read_excel(FINANCIAL_EXCEL_FILE_PATH)
financial_df = financial_df.fillna("N/A")

def convert_financial_revenue(value):
    if isinstance(value, str):
        if value.upper() == "N/A" or value == "-":
            return 0
        value = value.replace(",", "")
        try:
            return float(value)
        except ValueError:
            return 0
    return float(value)

financial_df["매출액"] = financial_df["매출액"].apply(convert_financial_revenue)

@app.get("/financial-annual-sales")
def get_financial_annual_sales(stock_name: str = Query(..., description="기업의 종목명")):
    try:
        if not stock_name.strip():
            return {"error": "종목명이 비어 있습니다.", "annual_sales": []}

        stock_name = stock_name.strip().lower()
        filtered_df = financial_df[financial_df["종목명"].str.lower() == stock_name]

        if filtered_df.empty:
            return {"error": f"종목명 '{stock_name}'을 찾을 수 없습니다.", "annual_sales": []}

        annual_sales = filtered_df[["연도", "매출액"]].copy()
        annual_sales["매출액"] = annual_sales["매출액"] / 10**8  # 억 단위
        annual_sales_sorted = annual_sales.sort_values(by="연도").to_dict(orient="records")

        return {"annual_sales": annual_sales_sorted}
    except Exception as e:
        logger.error(f"연간 매출액 조회 중 오류 발생: {e}")
        return {"annual_sales": [], "error": str(e)}


def convert_financial_operating_profit(value):
    if isinstance(value, str):
        if value.upper() == "N/A" or value.strip() == "-":
            return 0
        value = value.replace(",", "")
        try:
            return float(value)
        except ValueError:
            return 0
    return float(value) if pd.notnull(value) else 0

financial_df["영업이익"] = financial_df["영업이익"].apply(convert_financial_operating_profit)

@app.get("/financial-operating-profit")
def get_financial_operating_profit(stock_name: str = Query(..., description="기업의 종목명")):
    try:
        filtered_df = financial_df[financial_df["종목명"].str.lower() == stock_name.strip().lower()]
        if filtered_df.empty:
            return {"error": "해당 종목명을 찾을 수 없습니다.", "operating_profit": []}

        operating_profit = filtered_df[["연도", "영업이익"]].copy()
        operating_profit["영업이익"] = operating_profit["영업이익"] / 10**8
        operating_profit_sorted = operating_profit.sort_values(by="연도").to_dict(orient="records")
        
        return {"operating_profit": operating_profit_sorted}
    except Exception as e:
        logger.error(f"영업이익 조회 중 오류 발생: {e}")
        return {"operating_profit": [], "error": str(e)}


def convert_financial_net_income(value):
    if isinstance(value, str):
        if value.upper() == "N/A" or value.strip() == "-":
            return 0
        value = value.replace(",", "")
        try:
            return float(value)
        except ValueError:
            return 0
    return float(value) if pd.notnull(value) else 0

financial_df["당기순이익"] = financial_df["당기순이익"].apply(convert_financial_net_income)

@app.get("/financial-net-income")
def get_financial_net_income(stock_name: str = Query(..., description="기업의 종목명")):
    try:
        if not stock_name.strip():
            return {"error": "종목명이 비어 있습니다.", "순이익": []}

        stock_name = stock_name.strip().lower()
        filtered_df = financial_df[financial_df["종목명"].str.lower() == stock_name]
        if filtered_df.empty:
            return {"error": f"종목명 '{stock_name}'을 찾을 수 없습니다.", "순이익": []}

        net_income = filtered_df[["연도", "당기순이익"]].copy()
        net_income["순이익"] = net_income["당기순이익"] / 10**8
        net_income_sorted = net_income.sort_values(by="연도").to_dict(orient="records")

        return {"net_income": net_income_sorted}
    except Exception as e:
        logger.error(f"순이익 조회 중 오류 발생: {e}")
        return {"net_income": [], "error": str(e)}


def calculate_operating_income_rate(row):
    try:
        if row["매출액"] == 0:
            return 0.0
        return round((row["영업이익"] / row["매출액"]) * 100, 1)
    except Exception as e:
        logger.error(f"영업이익률 계산 중 오류 발생: {e}")
        return 0.0

financial_df["영업이익률"] = financial_df.apply(calculate_operating_income_rate, axis=1)

@app.get("/financial-operating-income-rate")
def get_financial_operating_income_rate(
    stock_name: str = Query(..., description="기업의 종목명")
):
    try:
        if not stock_name.strip():
            return {"error": "종목명이 비어 있습니다.", "operating_income_rate": []}

        stock_name = stock_name.strip().lower()
        filtered_df = financial_df[financial_df["종목명"].str.lower() == stock_name]
        if filtered_df.empty:
            return {"error": f"종목명 '{stock_name}'을 찾을 수 없습니다.", "operating_income_rate": []}

        operating_income_rate = filtered_df[["연도", "영업이익률"]].copy()
        operating_income_rate_sorted = operating_income_rate.sort_values(by="연도").to_dict(orient="records")

        return {"operating_income_rate": operating_income_rate_sorted}

    except Exception as e:
        logger.error(f"영업이익률 조회 중 오류 발생: {e}")
        return {"operating_income_rate": [], "error": str(e)}


def convert_financial_total_debt(value):
    if isinstance(value, str):
        if value.upper() == "N/A" or value.strip() == "-":
            return 0
        value = value.replace(",", "")
        try:
            return float(value)
        except ValueError:
            return 0
    return float(value) if pd.notnull(value) else 0

def convert_financial_total_equity(value):
    if isinstance(value, str):
        if value.upper() == "N/A" or value.strip() == "-":
            return 0
        value = value.replace(",", "")
        try:
            return float(value)
        except ValueError:
            return 0
    return float(value) if pd.notnull(value) else 0

financial_df["부채총계"] = financial_df["부채총계"].apply(convert_financial_total_debt)
financial_df["자본총계"] = financial_df["자본총계"].apply(convert_financial_total_equity)

def calculate_debt_ratio(row):
    try:
        if row["자본총계"] == 0:
            return 0.0
        return round((row["부채총계"] / row["자본총계"]) * 100, 1)
    except Exception as e:
        logger.error(f"부채비율 계산 중 오류 발생: {e}")
        return 0.0

financial_df["부채비율"] = financial_df.apply(calculate_debt_ratio, axis=1)

@app.get("/financial-debt-ratio")
def get_financial_debt_ratio(stock_name: str = Query(..., description="기업의 종목명")):
    try:
        if not stock_name.strip():
            return {"error": "종목명이 비어 있습니다.", "debt_ratio": []}

        stock_name = stock_name.strip().lower()
        filtered_df = financial_df[financial_df["종목명"].str.lower() == stock_name]
        if filtered_df.empty:
            return {"error": f"종목명 '{stock_name}'을 찾을 수 없습니다.", "debt_ratio": []}

        debt_ratio = filtered_df[["연도", "부채비율"]].copy()
        debt_ratio_sorted = debt_ratio.sort_values(by="연도").to_dict(orient="records")

        return {"debt_ratio": debt_ratio_sorted}
    except Exception as e:
        logger.error(f"부채비율 조회 중 오류 발생: {e}")
        return {"debt_ratio": [], "error": str(e)}


# python -m uvicorn main:app --reload
