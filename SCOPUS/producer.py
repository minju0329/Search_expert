import urllib.request
import re
import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from bs4 import BeautifulSoup



def crawing():
    # 웹드라이버를 통해 chrome open
    driver = webdriver.Chrome("./chromedriver")
    URL = "https://www.scopus.com/search/form.uri?display=basic&zone=header&origin="

    # URL 접속
    driver.get(URL)
    # 검색창 검색어 입력 / 검색
    search_box = driver.find_element_by_css_selector("input#searchterm1")
    search_box.send_keys("AI")
    search_button = driver.find_element_by_css_selector("#searchBtnRow > button").click()
    print("검색어 입력 성공")
    time.sleep(3)

    url = driver.current_url

    # 검색 결과 수 가져오기
    total_amount = driver.find_element_by_class_name("resultsCount").text
    print("총 결과 수:", total_amount)
    # 연도별 논문 수 가져오기
    try:
        viewMoreLink = driver.find_element_by_id("viewMoreLink_PUBYEAR").click()
        viewAllLink = driver.find_element_by_css_selector("#viewAllLink_PUBYEAR").click()
        print("연도별 논문 수 전체 보기")
    except:
        try:
            viewMoreLink = driver.find_element_by_class_name("_pendo-close-guide_").click()
            print("팝업창 닫기")

        except:
            viewMoreLink2 = driver.find_element_by_class_name("_pendo-close-guide").click()
            print("팝업창 닫기")
        viewMoreLink = driver.find_element_by_id("viewMoreLink_PUBYEAR").click()
        viewallLink = driver.find_element_by_id("viewAllLink_PUBYEAR").click()
        print("연도별 논문 수 전체 보기")

    time.sleep(3)
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    row_years = soup.select('div.row.body > ul > li > label.checkbox-label > span.btnText')
    # #============================================= html
    row_counts = soup.select('div.row.body > ul > li > button > span > span.btnText')

    year = []
    for i,j in zip(row_years, row_counts):
        year.append([i.text,j.text])

    print(year)


crawing()

