
import time
import re
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.support.wait import WebDriverWait


class SCOPUS:
    def __init__(self, f_query, query):
        self.chrome_options = webdriver.ChromeOptions()
        self.driver = webdriver.Chrome("./chromedriver86")
        self.total_amount   = 0
        self.download_count = 0
        self.popup = False
        self.end = False
        self.country_flag = False
        self.keyword_flag = False
        self.csv_flag = False
        self.papers_per_year = {}
        self.papers_per_language = {}
        self.papers_per_country = {}
        self.papers_per_keyword = {}
        self.over_2000_papers_county = []
        self.over_2000_papers_keyword = []
        self.sleepTime = 1
        self.timeOut = 5
        self.f_query = f_query
        self.query = query

    # ================================================================================

    def action(self, flag, _param, flag2, _param2):
        if self.popup == False:
            try:
                time.sleep(1)
                close = self.driver.find_element_by_class_name("_pendo-close-guide").click()
                self.popup = True
                print("팝업창 닫기")
            except:
                try:
                    close = self.driver.find_element_by_class_name("_pendo-close-guide_").click()
                    self.popup = True
                    print("팝업창 닫기")
                except:
                    # print("팝업창 없음")
                    pass

        if flag == 1:
            temp = WebDriverWait(self.driver, self.timeOut).until(lambda x: x.find_element_by_id(_param))
        elif flag == 2:
            temp = WebDriverWait(self.driver, self.timeOut).until(lambda x: x.find_element_by_css_selector(_param))
            # self.driver.find_element_by_css_selector(_param)
        elif flag == 3:
            temp = WebDriverWait(self.driver, self.timeOut).until(lambda x: x.find_element_by_class_name(_param))
            # self.driver.find_element_by_class_name(_param)
        elif flag == 4:
            temp = WebDriverWait(self.driver, self.timeOut).until(lambda x: x.find_element_by_xpath(_param))

        if flag2 == 1:
            time.sleep(self.sleepTime)
            temp.click()
        elif flag2 == 2:
            temp.send_keys(_param2)
        elif flag2 == 3:
            return temp.text

        # time.sleep(self.sleepTime)

    def checkModalOpen(self, _id):
        while True:
            if 'in' in self.driver.find_element_by_css_selector(_id).get_attribute('class').split():
                break
            time.sleep(self.sleepTime)

    def check_checkbox(self):
        if self.csv_flag == False:
            self.action(2, '#bibliographicalInformationCheckboxes > span > label', 1, None)
            self.action(2, '#abstractInformationCheckboxes > span > label', 1, None)
            self.action(2, '#fundInformationCheckboxes > span > label', 1, None)
            self.csv_flag = True
        self.action(2, '#exportTrigger', 1, None)

        self.download_count.value += 1
        print("다운로드:", self.download_count)

    def open_site(self):
        URL = "https://www.scopus.com/search/form.uri?display=basic&zone=header&origin="
        self.driver.get(URL)
        print(self.driver.title)
        print(self.driver.current_url)
        time.sleep(3)
        self.action(3, "secondaryLink", 1, None)
        self.action(1, "searchfield", 2, self.f_query)
        self.action(1, "advSearch", 1, None)
        print("검색어 입력 성공")

    # 검색 결과 수 가져오기
    def total_count(self):
        total_papers = self.action(2, "#searchResFormId > div:nth-child(2) > div > header > h1", 3, None)
        if 'Document search results' in total_papers:
            total_papers = 0
        else:
            total_papers = total_papers.split(' ')[0]
            self.total_amount = int(re.sub(',', '', total_papers))
        print("총 결과 수:", self.total_amount)
        return self.total_amount

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
            self.checkModalOpen('#navigatorOverlay_PUBYEAR')

            row_years = self.HTML('div.row.body > ul > li > label.checkbox-label > span.btnText')
            row_papers = self.HTML('div.row.body > ul > li > button > span.badge > span.btnText')
            self.action(2, "#resultViewMoreModalMainContent_PUBYEAR > div.modal-header > button", 1, None)
        except:
            # viewall or view more이 없을 경우
            row_years = self.HTML('#cluster_PUBYEAR > li.checkbox > label.checkbox-label > span.btnText')
            row_papers = self.HTML('#cluster_PUBYEAR > li > button > span.badge > span.btnText')

        for i, j in zip(row_years, row_papers):
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
                temp = ' EXCLUDE ' + '( ' + item + ',' + '\"' + i[0] + '\"' + ')'
                arr.append(temp)

        string = ' OR '.join(arr)
        new_query = ' AND (' + string + ' )'
        return new_query

    def download(self):
        print("댜운")
        # self.action(2, '#selectAllCheck', 1, None)
        # self.action(4, '//*[@id="export_results"]/span', 1, None)
        # self.action(4, '//*[@id="exportList"]/li[4]/label', 1, None)
        # self.check_checkbox()

    def re_search(self, re_query):
        self.action(1, 'editAuthSearch', 1, None)
        self.action(3, "secondaryLink", 1, None)
        try:
            self.action(1, 'clearLink', 1, None)
        except:
            self.action(2, "advSearchLink", 1, None)
        self.action(1, "searchfield", 2, re_query)
        print("")
        print("=================================================================")
        print(re_query)
        print("=================================================================")
        print("")
        self.action(1, "advSearch", 1, None)
        self.total_count()

    def get_attributePaper(self, collapse, viewMore, viewAll, Modal, Item, Paper, close, temp_dict, flag):
        if flag == False:
            self.action(2, collapse, 1, None)
            self.action(2, viewMore, 1, None)
            self.action(2, viewAll, 1, None)
            self.country_flag == True
        else:
            self.action(2, viewAll, 1, None)

        self.checkModalOpen(Modal)

        row_Item = self.HTML(Item)
        row_cnt = self.HTML(Paper)
        self.action(2, close, 1, None)

        for i, j in zip(row_Item, row_cnt):
            i = re.sub('\n', "", i.text)
            j = re.sub(',', "", j.text)
            temp_dict[i] = int(j)
        print("get_attr_Paper 종료")
        return temp_dict

    def under2000_years(self):
        year_papers = []
        for year in self.papers_per_year:
            year_papers.append([year, self.papers_per_year[year]])
        year_papers.sort(key=lambda x: x[1])

        while 1:
            cnt = 0
            end_idx = 0
            start_idx = 0
            if year_papers[start_idx][1] > 2000:  # 2000개 이하의 논문수 연도가 없으면 끝남
                self.over_2000_papers = year_papers[start_idx:]

                break

            for idx in range(start_idx, len(year_papers)):
                cnt = cnt + year_papers[idx][1]
                if cnt > 2000:
                    end_idx = idx
                    Temp = year_papers[start_idx:end_idx]
                    val = ([i[0] for i in Temp])  # 2000이하 연도 리스트
                    re_query = self.query + self.creat_query('PUBYEAR', val, True)
                    self.re_search(re_query)
                    self.download()
                    year_papers = year_papers[end_idx:]
                    break
            if cnt < 2000 and cnt != 0:
                val = (i[0] for i in year_papers)
                re_query = self.query + self.creat_query('PUBYEAR', val, True)
                self.re_search(re_query)
                self.download()
                self.over_2000_papers = []
                break

    def Access_type(self, ex_query, year):
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        open_Access = self.action(2, "#li_1 > button > span.badge > span.btnText", 3, None)
        other = self.action(2, "#li_0 > button > span.badge > span.btnText", 3, None)
        open_Access = int(re.sub(',', "", open_Access))
        other = int(re.sub(',', "", other))

        if open_Access <= 2000 and other <= 2000:  # 둘 다 2000개 이하일 때,
            re_query = ex_query + self.creat_query('openaccess', ['1'], True)
            self.re_search(re_query)
            self.download()

            re_query = ex_query + self.creat_query('openaccess', ['0'], True)
            self.re_search(re_query)
            self.download()
            return True, 1, 1

        elif open_Access <= 2000:  # open_Acccess만 이하인 경우
            re_query = ex_query + self.creat_query('openaccess', ['1'], True)
            self.re_search(re_query)
            self.download()
            return ex_query + self.creat_query('openaccess', ['0'], True), 0, other

        elif other <= 2000:
            re_query = ex_query + self.creat_query('openaccess', ['0'], True)
            self.re_search(re_query)
            self.download()
            return ex_query + self.creat_query('openaccess', ['1'], True), open_Access, 0
        else:
            return ex_query, open_Access, other

    def search_Access(self, year, type, typeCnt, query):
        query_access = year + type
        self.re_search(query_access)
        self.search_country(query_access, typeCnt)  # country를 exclude로 재검색
        self.country_flag = True
        for idx in range(len(self.over_2000_papers_country)):
            query_country = query_access + query + self.over_2000_papers_country[idx][0] + ') ) '
            self.re_search(query_country)
            self.search_keyword(query_country, self.over_2000_papers_country[idx][1])
            self.keyword_flag = True

    def search_country(self, query_access, Access_num):
        Tag = "#collapse_COUNTRY_NAME_link"
        More_Tag = "#viewMoreLink_COUNTRY_NAME > span"
        All_Tag = "#viewAllLink_COUNTRY_NAME > span"
        Modal = "#navigatorOverlay_COUNTRY_NAME"
        Countrys = "div.row.body > ul > li > label.checkbox-label > span.btnText"
        Papers = "div.row.body > ul > li > button > span.badge > span.btnText"
        Close = "#resultViewMoreModalMainContent_COUNTRY_NAME > div.modal-header > button"
        self.papers_per_country = self.get_attributePaper(Tag, More_Tag, All_Tag, Modal, Countrys, Papers, Close,
                                                          self.papers_per_country, self.country_flag)

        country_papers = []
        for country in self.papers_per_country:
            country_papers.append([country, self.papers_per_country[country]])

        country_papers.sort(key=lambda x: x[1], reverse=True)

        Country = []
        exclude_country = []
        for idx in range(len(country_papers)):
            Access_num = Access_num - country_papers[idx][1]
            Country.append(country_papers[idx])
            exclude_country.append(country_papers[idx])
            if Access_num < 2000:
                a = self.creat_query('AFFILCOUNTRY', Country, False)
                re_query = query_access + a
                self.re_search(re_query)
                total = self.total_count()
                if total > 2000:  # 검색한 결과가 2000이 넘는지 안넘는지 확인
                    self.search_keyword(re_query, total)  # keyword로 잘라서 다운로드
                else:
                    self.download()
                break
        exclude_country.sort(key=lambda x: x[1])

        while 1:
            cnt = 0
            end_idx = 0
            start_idx = 0
            if exclude_country[start_idx][1] > 2000:  # 2000개 이하의 논문수 나라가 없으면 끝남
                self.over_2000_papers_country = exclude_country
                break

            if cnt <= 2000 and sum(i[1] for i in exclude_country) <= 2000:
                val = [i[0] for i in exclude_country]
                re_query = query_access + self.creat_query('AFFILCOUNTRY', val, True)
                self.re_search(re_query)
                self.download()
                break

            if len(exclude_country) != 1 and sum(i[1] for i in exclude_country) > 2000:
                for idx in range(start_idx, len(exclude_country)):
                    cnt = cnt + exclude_country[idx][1]
                    if cnt > 2000:
                        end_idx = idx
                        Temp = exclude_country[start_idx:end_idx]
                        val = [i[0] for i in Temp]  # 2000이하 연도 리스트
                        # print(val)
                        re_query = query_access + self.creat_query('AFFILCOUNTRY', val, True)
                        self.re_search(re_query)
                        self.download()
                        exclude_country = exclude_country[end_idx:]
                        break

    def search_keyword(self, query_country, Access_num):
        Tag = "#collapse_EXACTKEYWORD_link"
        More_Tag = "#viewMoreLink_EXACTKEYWORD > span"
        All_Tag = "#viewAllLink_EXACTKEYWORD > span"
        Modal = "#navigatorOverlay_EXACTKEYWORD"
        Keywords = "div.row.body > ul > li > label.checkbox-label > span.btnText"
        Papers = "div.row.body > ul > li > button > span.badge > span.btnText"
        Close = "#resultViewMoreModalMainContent_EXACTKEYWORD > div.modal-header > button"
        self.papers_per_keyword = self.get_attributePaper(Tag, More_Tag, All_Tag, Modal, Keywords, Papers, Close,
                                                          self.papers_per_keyword, self.keyword_flag)

        keyword_papers = []
        for keyword in self.papers_per_keyword:
            keyword_papers.append([keyword, self.papers_per_keyword[keyword]])

        keyword_papers.sort(key=lambda x: x[1], reverse=True)

        Keyword = []
        exclude_Keyword = []

        for idx in range(len(keyword_papers)):
            Access_num = Access_num - keyword_papers[idx][1]
            Keyword.append(keyword_papers[idx])
            exclude_Keyword.append(keyword_papers[idx])
            if Access_num < 2000:
                a = self.creat_query('EXACTKEYWORD', Keyword, False)
                re_query = query_country + a
                self.re_search(re_query)
                self.download()
                break

        exclude_Keyword.sort(key=lambda x: x[1])

        while 1:
            cnt = 0
            end_idx = 0
            start_idx = 0
            if exclude_Keyword[0][1] > 2000:  # 2000개 이하의 논문수 나라가 없으면 끝남
                self.over_2000_papers_keyword = exclude_Keyword[start_idx:]
                break

            if cnt <= 2000 and sum(i[1] for i in exclude_Keyword) <= 2000:
                val = [i[0] for i in exclude_Keyword]
                re_query = query_country + self.creat_query('EXACTKEYWORD', val, True)
                self.re_search(re_query)
                self.download()
                break

            if len(exclude_Keyword) != 1 and sum(i[1] for i in exclude_Keyword) > 2000:
                for idx in range(start_idx, len(exclude_Keyword)):
                    cnt = cnt + exclude_Keyword[idx][1]
                    if cnt > 2000:
                        end_idx = idx
                        Temp = exclude_Keyword[start_idx:end_idx]
                        val = [i[0] for i in Temp]  # 2000이하 연도 리스트
                        # print(val)
                        re_query = query_country + self.creat_query('EXACTKEYWORD', val, True)
                        self.re_search(re_query)
                        self.download()
                        exclude_Keyword = exclude_Keyword[end_idx:]
                        break

    def search_Access(self, year, type, typeCnt, query):
        query_access = year + type
        self.re_search(query_access)
        self.search_country(query_access, typeCnt)  # country를 exclude로 재검색
        self.country_flag = True
        for idx in range(len(self.over_2000_papers_country)):
            query_country = query_access + query + self.over_2000_papers_country[idx][0] + ') ) '
            self.re_search(query_country)
            self.search_keyword(query_country, self.over_2000_papers_country[idx][1])
            self.keyword_flag = True

    def over2000_years(self, start):
        min = (time.time() - start) // 60
        print("")
        print("=================================================================")
        print('2000개 이하 년도 완료', "time:", min, '분')
        print("=================================================================")
        print("")
        for i in self.over_2000_papers:
            self.over_2000_papers_country = []
            temp = self.query
            query_year = temp + ' ' + ' AND ( LIMIT-TO ( PUBYEAR, ' + i[0] + ') ) '
            self.re_search(query_year)  # 2000개 이상의 연도 재검색

            requery, open_Access, other = self.Access_type(query_year, i[0])  # Access type 논문 수 파악
            aceessType_1 = 'AND ( LIMIT-TO ( openaccess,1))'
            aceessType_2 = 'AND ( LIMIT-TO ( openaccess,0))'
            query_limit = ' ', ' AND ( LIMIT-TO ( AFFILCOUNTRY, '  # ' '+ ' AND ( LIMIT-TO ( AFFILCOUNTRY, '

            if type == True:
                pass

            elif (open_Access != 0 and other != 0):
                self.search_Access(query_year, aceessType_1, open_Access, query_limit)
                self.over_2000_papers_country = []
                self.search_Access(query_year, aceessType_2, other, query_limit)

            elif other == 0 and requery != True:
                self.search_Access(query_year, aceessType_1, open_Access, query_limit)

            elif open_Access == 0 and requery != True:
                self.search_Access(query_year, aceessType_2, other, query_limit)

            min = (time.time() - start) // 60
            print("")
            print("=============================================================")
            print(i[0], '년 완료', "time: ", min, '분')
            print("=============================================================")
            print("")
            #     break
            # except Exception as e :
            #     print(e)
            #     retryCount -= 1
        return 0

def create_queary():
    keywords = {'and': ["fuel", "cell", "membrane"], 'or': [], 'not': [], 'year': [2010]}
    s_queary = ''
    and_not = ''
    if len(keywords['not']) == 1:
        and_not = 'AND NOT ' + keywords['not'][0]
    elif len(keywords['not']) > 1:
        and_not = 'AND NOT ' + ' AND NOT '.join(keywords['not'])

    if len(keywords['and']) > 0:
        s_queary = ' AND '.join(keywords['and']) + ' ' + and_not

    else:
        s_queary = ' OR '.join(keywords['or']) + ' ' + and_not

    queary = 'TITLE-ABS-KEY ( ' + s_queary + ' )'
    f_queary = 'TITLE-ABS-KEY ( ' + s_queary + ' )  AND  PUBYEAR  >  ' + str(keywords['year'][0] - 1)

    return f_queary, queary

def main():
    f_Queary, Queary = create_queary()
    site = SCOPUS(f_Queary, Queary)
    start = time.time()
    site.open_site()
    site.total_count()
    if site.total_amount <= 2000:
        return 0
    site.years()            # 년도별 논문 수 딕셔너리
    print('years완료')
    site.under2000_years()
    site.over2000_years(start)
    print("종료")
    time.sleep(100)
    site.driver.quit()


if __name__ == "__main__":
    main()
