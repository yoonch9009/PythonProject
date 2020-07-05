#!/usr/bin/env python
# coding: utf-8

# In[1]:


from selenium import webdriver #selenium
from selenium.webdriver import Chrome #chrome 브라우저 사용
from selenium.webdriver.chrome.options import Options #chrome headless 옵션 사용
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait #wait 사용
from selenium.webdriver.common.by import By #암시적 wait
from selenium.webdriver.support import expected_conditions as EC #암시적 wait
from selenium.common.exceptions import StaleElementReferenceException #사이트 불러오기 exception
from selenium.common.exceptions import NoSuchElementException #사이트 불러오기 exception2
from selenium.webdriver.support.select import Select # select 태그 조작


import requests # 웹 페이지 소스를 얻기 위한 패키지(기본 내장 패키지이다.)
from bs4 import BeautifulSoup # 웹 페이지 소스를 얻기 위한 패키지, 더 간단히 얻을 수 있다는 장점이 있다고 한다.
from datetime import datetime                                # (!pip install beautifulsoup4 으로 다운받을 수 있다.)
import pandas as pd # 데이터를 처리하기 위한 가장 기본적인 패키지
import numpy as np # 데이터를 처리하기 위한 가장 기본적인 패키지2
import time # 사이트를 불러올 때, 작업 지연시간을 지정해주기 위한 패키지이다. (사이트가 늦게 켜지면 에러가 발생하기 때문)
import urllib.request #http 요청
import urllib.parse #http 파싱

import json
import re  #정규표현식

import sqlite3 #DB sqlite 사용
import datetime as dt #데이터베이스 저장할때 오늘날짜 체크용


# KRX의 각 시장 데이터 파싱(kospi_stocks, kosdaq_stocks)

# In[2]:


import urllib.parse


def download_stock_codes(market=None, delisted=False):#KRX 주식데이터 파싱
    MARKET_CODE_DICT = {
    'kospi': 'stockMkt',
    'kosdaq': 'kosdaqMkt'
    }
    STOCK_DOWNLOAD_URL = 'kind.krx.co.kr/corpgeneral/corpList.do'
    
    params = {'method': 'download'}

    if market.lower() in MARKET_CODE_DICT:
        params['marketType'] = MARKET_CODE_DICT[market]

    if not delisted:
        params['searchType'] = 13

    params_string = urllib.parse.urlencode(params)
    request_url = urllib.parse.urlunsplit(['http', STOCK_DOWNLOAD_URL, '', params_string, ''])

    df = pd.read_html(request_url, header=0)[0]
    df.종목코드 = df.종목코드.map('{:06d}'.format)

    return df


def stocks_to_dataframe(market): #주식데이터에 퀀트데이터 저장을 위해 columns 변경
    dataframe = download_stock_codes(market)#주식데이터 파싱
    dataframe = dataframe[['종목코드','회사명','업종','주요제품','지역']]
    dataframe.columns = ['code','name','industry','product','location']
    dataframe['market'] = market
    dataframe['price'] = np.nan
    dataframe['cap'] = np.nan
    dataframe['EPS'] = np.nan
    dataframe['BPS'] = np.nan
    dataframe['CPS'] = np.nan
    dataframe['SPS'] = np.nan
    dataframe['PER'] = np.nan
    dataframe['PBR'] = np.nan
    dataframe['PCR'] = np.nan
    dataframe['PSR'] = np.nan
    dataframe['EV_EBITDA'] = np.nan
    dataframe['ROE'] = np.nan
    dataframe['ROA'] = np.nan
    dataframe['ROIC'] = np.nan
    dataframe['D_E'] = np.nan
    dataframe['C_R'] = np.nan
    dataframe['GP_A'] = np.nan
    return dataframe
    
kospi_stocks = stocks_to_dataframe('kospi')
kosdaq_stocks = stocks_to_dataframe('kosdaq')


# kospi_stocks = kospi_stocks.set_index(kospi_stocks['code'])
# kosdaq_stocks = kosdaq_stocks.set_index(kosdaq_stocks['code'])


# Selenium 사용해서 주식데이터 크롤링

# In[3]:



#selenium headless 설정
chromedriver = '/usr/bin/chromedriver'
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('window-size=1920x1080')
options.add_argument("disable-gpu")
driver = webdriver.Chrome(chromedriver, options=options)

#정규표현식
number = re.compile(r"[^-|.|0-9]") #-, ., 숫자 외 제거


class quant_screener:
        
    
    def __init__(self, code):
        self.code = code
        
        self.price = 0
        self.cap = 0
        
        self.EPS = 0
        self.BPS = 0
        self.CPS = 0
        self.SPS = 0
        self.PER = 0
        self.PBR = 0
        self.PCR = 0
        self.PSR = 0
        self.EV_EBITDA = 0
        
        self.ROE = 0
        self.ROA = 0
        self.ROIC = 0
        
        self.D_E = 0 #부채비율
        self.C_R = 0 #유동비율
        
        self.GP_A = 0 #매출총이익/자산총계
        
        
    ######실시간 가격 데이터 필요할때######################
    def open_price_naver(self):#네이버 실시간 종목가격
        url = 'https://m.stock.naver.com/item/main.nhn#/stocks/'+ self.code +'/total'
        driver.get(url)#종목 입력
#         driver.implicitly_wait(3)#3초 대기
#         price_tag = driver.find_element_by_xpath('//*[@id="header"]/div[4]/div[1]/div/div[2]/div/div[2]/div[1]/strong')
        price_tag = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="header"]/div[4]/div[1]/div/div[2]/div/div[2]/div[1]/strong')))
        self.price = price_tag.get_attribute('data-current-price')#현재가격 속성 가져오기
        
        
        
    def open_price(self):#wise 주가, 시총 가져오기(wise 데이터는 전날 데이터임)
        url = 'https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd='+ self.code
        driver.get(url)#종목 입력
#         driver.implicitly_wait(3)#3초 대기
#         self.price = driver.find_element_by_xpath('/html/body/div/form/div[1]/div/div[2]/div[3]/div/div/div[1]/div[1]/div[2]/table/tbody/tr[1]/td/strong').text
        self.price = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/div/form/div[1]/div/div[2]/div[3]/div/div/div[1]/div[1]/div[2]/table/tbody/tr[1]/td/strong'))).text
        self.price = float(number.sub("", self.price))
        self.cap = driver.find_element_by_xpath('/html/body/div/form/div[1]/div/div[2]/div[3]/div/div/div[1]/div[1]/div[2]/table/tbody/tr[5]/td').text
        self.cap = float(number.sub("", self.cap)) * 100000000 #단위 : 억원
        
    def open_wise(self):#종목 열기 : 네이버 금융 -> wise 리포트 -> 투자지표 
        url = 'https://navercomp.wisereport.co.kr/v2/company/c1040001.aspx?cmp_cd='+ self.code
        driver.get(url)#종목 입력
        driver.implicitly_wait(3)#3초 대기
        #WebDriverWait(driver, 10).until(EC.visibility_of_any_elements_located((By.XPATH, '//*[@id="finGubun"]')))
        
    def kifrs_select(self):# 회계방식 선택
        select = Select(driver.find_element_by_xpath('//*[@id="finGubun"]'))#위 select
        select.select_by_index(0)#K-IFRS(연결)
        driver.find_element_by_xpath('//*[@id="hfinGubun"]').click()#검색 버튼 클릭
        
        select = Select(driver.find_element_by_xpath('//*[@id="finGubun2"]'))#아래 select
        select.select_by_index(0)#K-IFRS(연결)
        driver.find_element_by_xpath('//*[@id="hfinGubun2"]').click()#검색 버튼 클릭
    
    def Profit(self):#수익성 탭
        driver.find_element_by_xpath('//*[@id="val_tab1"]').click()
        
    def Growth(self):#성장성 탭
        driver.find_element_by_xpath('//*[@id="val_tab2"]').click()
        
    def Stability(self):#안정성 탭
        driver.find_element_by_xpath('//*[@id="val_tab3"]').click()
        
    def Activity(self):#활동성 탭
        driver.find_element_by_xpath('//*[@id="val_tab4"]').click()
        
    def price_value(self):#가격지표 크롤링
        time.sleep(0.1)
        self.EPS = driver.find_element_by_xpath('/html/body/div/form/div[1]/div/div[2]/div[3]/div/div/div[9]/table[2]/tbody/tr[1]/td[6]').text
        self.BPS = driver.find_element_by_xpath('/html/body/div/form/div[1]/div/div[2]/div[3]/div/div/div[9]/table[2]/tbody/tr[5]/td[6]').text
        self.CPS = driver.find_element_by_xpath('/html/body/div/form/div[1]/div/div[2]/div[3]/div/div/div[9]/table[2]/tbody/tr[9]/td[6]').text
        self.SPS = driver.find_element_by_xpath('/html/body/div/form/div[1]/div/div[2]/div[3]/div/div/div[9]/table[2]/tbody/tr[13]/td[6]').text
        self.EV_EBITDA = driver.find_element_by_xpath('/html/body/div/form/div[1]/div/div[2]/div[3]/div/div/div[9]/table[2]/tbody/tr[29]/td[6]').text
        self.EPS = data_to_float(self.EPS)
        self.BPS = data_to_float(self.BPS)
        self.CPS = data_to_float(self.CPS)
        self.SPS = data_to_float(self.SPS)
        self.EV_EBITDA = data_to_float(self.EV_EBITDA)
        self.PER = safe_div(self.price, self.EPS)
        self.PBR = safe_div(self.price, self.BPS)
        self.PCR = safe_div(self.price, self.CPS)
        self.PSR = safe_div(self.price, self.SPS)
        
    def profit_value(self):#수익성 지표 크롤링
        self.Profit()
        time.sleep(0.1)
        self.ROE = driver.find_element_by_xpath('/html/body/div/form/div[1]/div/div[2]/div[3]/div/div/div[5]/table[2]/tbody/tr[13]/td[6]').text
        self.ROA = driver.find_element_by_xpath('/html/body/div/form/div[1]/div/div[2]/div[3]/div/div/div[5]/table[2]/tbody/tr[17]/td[6]').text
        self.ROIC = driver.find_element_by_xpath('/html/body/div/form/div[1]/div/div[2]/div[3]/div/div/div[5]/table[2]/tbody/tr[21]/td[6]').text
        
        self.ROE = data_to_float(self.ROE)
        self.ROA = data_to_float(self.ROA)
        self.ROIC = data_to_float(self.ROIC)
        
    def stability_value(self):#안정성 지표 크롤링
        self.Stability()
        time.sleep(0.1)
        self.D_E = driver.find_element_by_xpath('/html/body/div/form/div[1]/div/div[2]/div[3]/div/div/div[5]/table[2]/tbody/tr[1]/td[6]').text
        self.C_R = driver.find_element_by_xpath('/html/body/div/form/div[1]/div/div[2]/div[3]/div/div/div[5]/table[2]/tbody/tr[13]/td[6]').text
        self.D_E = data_to_float(self.D_E)
        self.C_R = data_to_float(self.C_R)
        

def data_to_float(data): #퀀트데이터 숫자 float 변경
    if( number.sub("", data) ==  "" ):
        data = 0.0
    else:
        data = float(number.sub("", data))
    return data
        
def insert_to_df(dataframe, count, quant): #크롤링한 데이터를 데이터프레임에 저장
    dataframe.loc[count, 'price'] = quant.price
    dataframe.loc[count, 'cap'] = quant.cap
    dataframe.loc[count, 'EPS'] = quant.EPS
    dataframe.loc[count, 'BPS'] = quant.BPS
    dataframe.loc[count, 'CPS'] = quant.CPS
    dataframe.loc[count, 'SPS'] = quant.SPS
    dataframe.loc[count, 'PER'] = quant.PER
    dataframe.loc[count, 'PBR'] = quant.PBR
    dataframe.loc[count, 'PCR'] = quant.PCR
    dataframe.loc[count, 'PSR'] = quant.PSR
    dataframe.loc[count, 'EV_EBITDA'] = quant.EV_EBITDA
    dataframe.loc[count, 'ROE'] = quant.ROE
    dataframe.loc[count, 'ROA'] = quant.ROA
    dataframe.loc[count, 'ROIC'] = quant.ROIC
    dataframe.loc[count, 'D_E'] = quant.D_E
    dataframe.loc[count, 'C_R'] = quant.C_R
    dataframe.loc[count, 'GP_A'] = quant.GP_A
    
def safe_div(x, y): #0으로 나누면 0처리
    if (y==0):
        return 0
    return x / y



try:
    for count, code in enumerate(kospi_stocks['code']):  # market 별 code 조회
        while True: #페이지 로딩 오류시 다시 요청
            try:
                q1 = quant_screener(code)#종목넣기

                q1.open_price()#주가, 시총

                q1.open_wise()#재무제표

                q1.kifrs_select()#k-ifrs 회계방식 선택(위, 아래)

                q1.price_value()#가격 지표
                q1.profit_value()#수익성 지표
                q1.stability_value()#안정성 지표

                insert_to_df(kosdaq_stocks, count, q1) # df 에 데이터 입력
                break

            except StaleElementReferenceException: #페이지 로딩 오류발생
                print("수신오류", end="")
                
            except NoSuchElementException: #페이지 로딩 오류발생2
                print("NoEle수신오류", end="")

            except Exception as ex:
                print('에러가 발생 했습니다', ex)
                break


        print('.', end='')#데이터 크롤링 진행 확인용
        if((count+1)%100 == 0):
            print(count+1)


    print('KOSPI 수신완료') #모든데이터 수신완료
    
    for count, code in enumerate(kosdaq_stocks['code']):  # market 별 code 조회
        while True: #페이지 로딩 오류시 다시 요청
            try:
                q1 = quant_screener(code)#종목넣기

                q1.open_price()#주가, 시총

                q1.open_wise()#재무제표

                q1.kifrs_select()#k-ifrs 회계방식 선택(위, 아래)

                q1.price_value()#가격 지표
                q1.profit_value()#수익성 지표
                q1.stability_value()#안정성 지표

                insert_to_df(kosdaq_stocks, count, q1) # df 에 데이터 입력
                break

            except StaleElementReferenceException: #페이지 로딩 오류발생
                print("수신오류", end="")
                
            except NoSuchElementException: #페이지 로딩 오류발생2
                print("NoEle수신오류", end="")

            except Exception as ex:
                print('에러가 발생 했습니다', ex)
                break


        print('.', end='')#데이터 크롤링 진행 확인용
        if((count+1)%100 == 0):
            print(count+1)


    print('KOSDAQ 수신완료') #모든데이터 수신완료


finally:
    driver.quit() #selenium 종료
    print('driver 종료')



# 수집된 Dataframe을 DB로 저장

# In[39]:


import sqlite3
import datetime as dt

def today():
    today_kst = dt.datetime.now() + dt.timedelta(hours=9)#한국시간 설정
    time_format = '%y%m%d' #'%y%m%d_%H%M'
    return today_kst.strftime(time_format)

con = sqlite3.connect('./database/' + today() + '_quant_data.db')#db 생성
cursor = con.cursor()#커서 생성

try:
    kospi_stocks.to_sql('QUANT', con, if_exists='replace')#코스피 자료 sqlite 전송
    kosdaq_stocks.to_sql('QUANT', con, if_exists='append')#코스닥 자료 sqlite 전송 (if_exists={fail, replace, append}) table이 이미 존재할때 처리방법
    readed_df = pd.read_sql("SELECT * FROM QUANT", con) #데이터 확인을 위해 DB 데이터 읽기
    #print(readed_df)
finally:
    con.commit()
#     con.close()


# In[ ]:


# con.execute("SELECT * FROM QUANT").fetchall()


# In[382]:


#kospi_stocks

