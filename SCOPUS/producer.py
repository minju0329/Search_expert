from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import re

class SCOPUS:
    def __init__(self):
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.driver = webdriver.Chrome("./chromedriver")
        # self.driver = webdriver.Chrome("/home/search/apps/dw/chromedriver",chrome_options=self.chrome_options)
        # d.get('https://www.google.nl/')
        self.total_amount=0
        self.popup = False
        self.papers_per_year = {}
        self.papers_per_language = {}
        self.papers_per_country = {}
        self.over_2000_papers = []
        self.sleepTime = 2
        self.query = 'TITLE-ABS-KEY ( ai )  AND  DOCTYPE ( ar )  AND  ( LIMIT-TO ( PUBYEAR ,  2017 ) ) '

    def action(self, flag, _param, flag2, _param2):
        if self.popup == False:
            try:
                close = self.driver.find_element_by_class_name("_pendo-close-guide").click()
                self.popup = True
                print("팝업창 닫기")
            except:
                try:
                    close = self.driver.find_element_by_class_name("_pendo-close-guide_").click()
                    self.popup = True
                    print("팝업창 닫기")
                except:
                    print("팝업창 없음")
                    pass

        if flag == 1:
            temp = self.driver.find_element_by_id(_param)
        elif flag == 2:
            temp = self.driver.find_element_by_css_selector(_param)
        elif flag == 3 :
            temp = self.driver.find_element_by_class_name(_param)

        if flag2 == 1:
            temp.click()
        elif flag2 == 2:
            temp.send_keys(_param2)
        elif flag2 == 3:
            return temp.text

        time.sleep(self.sleepTime)

    def open_site(self):
        URL = "https://www.scopus.com/search/form.uri?display=basic&zone=header&origin="
        self.driver.get(URL)
        time.sleep(5)
        self.action(3, "secondaryLink", 1, None)
        self.action(1, "searchfield", 2, self.query)
        self.action(3, "secondaryLink", 1, None)
        self.action(1, "advSearch", 1, None)
        print("검색어 입력 성공")
        #
        # search_button = driver.find_element_by_css_selector("#searchBtnRow > button")
        # try:
        #     close = driver.find_element_by_id("_pendo-close-guide_").click()
        #     print("팝업창 닫기")
        # except:
        #     pass
        # search_button.click()

    # 검색 결과 수 가져오기
    def total_count(self):
        total_papers = self.action(3, "resultsCount", 3, None)
        self.total_amount = int(re.sub(',', "", total_papers))
        print("총 결과 수:", self.total_amount)


    def HTML(self, _tag):
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        return soup.select(_tag)

    def years(self):
        # 연도별 논문 수 가져오기
        try:
            self.action(2, "#viewMoreLink_PUBYEAR", 1, None)
            self.action(2, "#viewAllLink_PUBYEAR", 1, None)
            print("연도별 논문 수 전체 보기")
            row_years = self.HTML('div.row.body > ul > li > label.checkbox-label > span.btnText')
            row_papers = self.HTML('div.row.body > ul > li > button > span.badge > span.btnText')
            self.action(2, "#resultViewMoreModalMainContent_PUBYEAR > div.modal-header > button", 1, None)
        except:
            # viewall or view more이 없을 경우
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            row_years = soup.select('#cluster_PUBYEAR > li.checkbox > label.checkbox-label > span.btnText')
            row_papers = soup.select('#cluster_PUBYEAR > li > button > span.badge > span.btnText')

        for i,j in zip(row_years, row_papers):
            i = re.sub('\n', "", i.text)
            j = re.sub(',', "", j.text)
            self.papers_per_year[i] = int(j)
        print("창닫기 성공")

    def creat_query(self, item, values, limit):
        new_query = ''
        arr = []
        if limit == True:
            for i in values:
                temp = ' LIMIT-TO ' + '( ' + item + ',' + i + ')'
                arr.append(temp)
        else:
            for i in values:
                section = str(i[0])
                temp = ' EXCLUDE ' + '( ' + item + ',' + section + ')'
                arr.append(temp)

        string = ' OR '.join(arr)
        new_query = ' AND (' + string + ' )'
        return new_query        # AND  ( LIMIT-TO ( PUBYEAR ,  2008 )  OR  LIMIT-TO ( PUBYEAR ,  2007 )  OR  LIMIT-TO ( PUBYEAR ,  2006 ) )

    def re_search(self,re_query):
        self.action(1, 'editAuthSearch', 1, None)
        self.action(1, 'clearLink', 1, None)
        self.action(1, "searchfield", 2, re_query)
        self.action(3, "secondaryLink", 1, None)
        self.action(1, "advSearch", 1, None)

    def under2000_years(self):
        # dic --> list 변환
        temp = []
        for year in self.papers_per_year:
            temp.append([year, self.papers_per_year[year]])
        temp.sort()
        # 2000개 이하 download
        while 1:
            cnt = 0
            end_idx = 0
            start_idx = 0
            # self.query = 'TITLE-ABS-KEY ( ai )  AND  DOCTYPE ( ar )'
            if temp[start_idx][1] > 2000:       # 2000개 이하의 논문수 연도가 없으면 끝남
                self.over_2000_papers = temp[start_idx:]
                break

            for idx in range(start_idx, len(temp)):
                cnt = cnt + temp[idx][1]
                if cnt > 2000:
                    end_idx = idx
                    Temp = temp[start_idx:end_idx]
                    val = ([i[0] for i in Temp])
                    re_query = self.query + self.creat_query('PUBYEAR', val, True)
                    temp = temp[end_idx:]
                    break


    def Access_type(self, ex_query):
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        open_Access = self.action(2, "#li_1 > button > span.badge > span.btnText", 3, None)
        other = self.action(2, "#li_0 > button > span.badge > span.btnText", 3, None)
        open_Access = int(re.sub(',', "", open_Access))
        other = int(re.sub(',', "", other))

        if open_Access <= 2000 and other <= 2000:   # 둘 다 2000개 이하일 때,
            # print(self.query, self.creat_query('openaccess', [[1,0]], True))
            re_query = ex_query+self.creat_query('openaccess', ['1'], True)
            self.re_search(re_query)
            print("open_Access 완료")
            re_query = ex_query + self.creat_query('openaccess', ['0'], True)
            self.re_search(re_query)
            print("other 완료")
            return True

        elif open_Access <=2000:        # open_Acccess만 이하인 경우
            re_query = ex_query + self.creat_query('openaccess', ['1'], True)
            self.re_search(re_query)
            print("open_Access 완료")
            #other 쪼개기
            return ex_query + self.creat_query('openaccess', ['0'],True)

        elif other <= 2000:
            re_query = ex_query + self.creat_query('openaccess', ['0'], True)
            self.re_search(re_query)
            print("other 완료")
            #open_Access 쪼개기
            return ex_query + self.creat_query('openaccess', ['1'], True)
        else:
            return ex_query

    def search_country(self):
        self.action(2, "#viewMoreLink_COUNTRY_NAME", 1, None)
        self.action(2, "#viewAllLink_COUNTRY_NAME", 1, None)
        print("나라별 논문 수 전체 보기")
        row_cont = self.HTML('div.row.body > ul > li > label.checkbox-label > span.btnText')
        row_cont_papers = self.HTML('div.row.body > ul > li > button > span.badge > span.btnText')
        self.action(2, "#resultViewMoreModalMainContent_COUNTRY_NAME > div.modal-header > button", 1, None)
        for i, j in zip(row_cont, row_cont_papers):
            i = re.sub('\n', "", i.text)
            j = re.sub(',', "", j.text)
            self.papers_per_country[i] = int(j)
        print(self.papers_per_country)

    def search_laguage(self):
        self.action(2, "#viewMoreLink_LANGUAGE", 1, None)
        self.action(2, "#viewAllLink_LANGUAGE", 1, None)
        print("언어별 논문 수 전체 보기")
        row_cont = self.HTML('div.row.body > ul > li > label.checkbox-label > span.btnText')
        row_cont_papers = self.HTML('div.row.body > ul > li > button > span.badge > span.btnText')
        self.action(2, "#resultViewMoreModalMainContent_LANGUAGE > div.modal-header > button", 1, None)
        for i, j in zip(row_cont, row_cont_papers):
            i = re.sub('\n', "", i.text)
            j = re.sub(',', "", j.text)
            self.papers_per_language[i] = int(j)

    def over2000_years(self):
        for i in self.over_2000_papers:        #[['2008', 2042], ['2009', 2078], ['2010', 2101], ['2011', 2331]]
            self.query = 'TITLE-ABS-KEY ( ai )  AND  DOCTYPE ( ar ) AND '+ '(LIMIT-TO(PUBYEAR, '+i[0]+'))'
            self.re_search(self.query)      # 2000개 이상의 연도 재검색
            self.Access_type(self.query)              # Access type 논문 수 파악
            self.query = self.re_search(self.query)      # 2000개 이상 Access_type, True, query1,2,3
            self.search_country()           # country를 exclude로 재검색

def main():
    site = SCOPUS()
    site.open_site()
    site.total_count()
    if site.total_amount <= 2000:
        print("프로그램 종료")
        return
    site.years()
    site.under2000_years()
    print(site.over_2000_papers)
    site.over2000_years()
    # site.Access_type()


if __name__ == "__main__":
    main()
