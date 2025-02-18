import time
from fastapi import FastAPI
import uvicorn

# Selenium 관련 임포트
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

app = FastAPI()

@app.get("/test-news")
def test_news():
    """
    1) 네이버 메인 접속
    2) '삼성전자' 검색
    3) '뉴스 더보기' 클릭
    4) 스크롤을 내려 30개 이상 기사 로드
    5) 상위 30개 기사 제목 추출 후 터미널에 출력하고 JSON으로 반환
    """
    # 크롬 옵션 설정 (헤드리스 모드)
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")            # 화면 없이 동작
    options.add_argument("--no-sandbox")          
    options.add_argument("--disable-dev-shm-usage")

    # webdriver 실행 (ChromeDriver 필요)
    driver = webdriver.Chrome(options=options)

    try:
        # (1) 네이버 메인 페이지 접속
        driver.get("https://www.naver.com/")
        time.sleep(2)

        # (2) 검색어 '삼성전자' 입력 후 엔터
        search_box = driver.find_element(By.NAME, "query")
        search_box.send_keys("삼성전자")
        search_box.send_keys(Keys.ENTER)
        time.sleep(2)

        # (3) '뉴스 더보기' 링크 클릭
        news_more_link = driver.find_element(By.PARTIAL_LINK_TEXT, "뉴스 더보기")
        news_more_link.click()
        time.sleep(2)

        # (4) 스크롤을 내려 30개 이상 기사 로드
        for _ in range(5):  # 최대 5번 스크롤
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            news_titles = driver.find_elements(By.CSS_SELECTOR, "a.news_tit")
            if len(news_titles) >= 30:
                break

        # (5) 상위 30개 기사 제목 추출
        final_titles = [elem.text for elem in news_titles[:30]]
        
        # 터미널에 출력
        print("추출된 뉴스 제목 30개:")
        for title in final_titles:
            print(title)
        
        return {"titles": final_titles}

    finally:
        driver.quit()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
