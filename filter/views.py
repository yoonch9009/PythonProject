from django.shortcuts import render
import json
import pandas as pd
import os
import sqlite3
import datetime as dt

def today():
    today_kst = dt.datetime.now() + dt.timedelta(hours=9)#한국시간 설정
    time_format = '%y%m%d' #'%y%m%d_%H%M'
    return today_kst.strftime(time_format)

# 오늘 날짜 디비 확인
def isDatabase(BASE_PATH):
    if os.path.exists(BASE_PATH):
        return True
    return False
     
def index(request):
    result = []
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    BASE_PATH = BASE_DIR + "/database/" + today() + "_quant_data.db"    

    if (isDatabase(BASE_PATH) is True):                
        connection = sqlite3.connect(BASE_PATH)
        query = connection.cursor()
        query.execute("SELECT * FROM QUANT")
        rows = query.fetchall()               
        for row in rows:
            result.append({
                "id": '{0:06d}'.format(row[1]),
                "industry": row[3],            
                "location": row[5],
                "name": row[2],
                "price": row[7],
                "cap": row[8],
                "per": row[13],
                "pbr": row[14],
                "pcr": row[15],
                "psr": row[16],
                "roe": row[18],
                "roa": row[19],
                "roic": row[20],
                "de": row[21],
                "cr": row[22],
            })
    else:
        xlsx = pd.read_excel(BASE_DIR + '/filter/QUANT.xlsx')

        for index, row in xlsx.iterrows():
            result.append({
                "id": '{0:06d}'.format(row[1]),
                "industry": row[3],            
                "location": row[5],
                "name": row[2],
                "price": row[7],
                "cap": row[8],
                "per": row[13],
                "pbr": row[14],
                "pcr": row[15],
                "psr": row[16],
                "roe": row[18],
                "roa": row[19],
                "roic": row[20],
                "de": row[21],
                "cr": row[22],
            })
        
    return render(request, 'index.html', {'data': json.dumps(result)})