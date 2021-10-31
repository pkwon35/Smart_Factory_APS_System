#### 수정필요
## 1) 시작일자에 따라 w 의 평균 값이 바뀜. 유훈이랑 확인 필요
from datetime import timedelta
import sqlalchemy
import pandas as pd
from sqlalchemy.ext.declarative import declarative_base


## 실행전 위치 지정
# ==============================================================================================================
# ==============================================================================================================
# ==============================================================================================================
# ==============================================================================================================

#    망원옥상  ,  학원  ,  망원701호  ,  등등..
현재위치 = '망원옥상'
# Define the MariaDB engine using MariaDB Connector/Python

##### db 연결시 필요한 사용자함수  연결하는 곳 마다 실행하는 사용자함수가 달라진다.

if 현재위치 == '망원옥상':
    engine = sqlalchemy.create_engine("mariadb+mariadbconnector://smart_factory_team:0000@192.168.200.190:3306/smart_factory")
    Base = declarative_base()
    Base.metadata.create_all(engine)

if 현재위치 == '망원701호':
    engine = sqlalchemy.create_engine("mariadb+mariadbconnector://smart_factory_team:0000@192.168.200.100:3306/smart_factory")
    Base = declarative_base()
    Base.metadata.create_all(engine)

if 현재위치 == '학원':
    engine = sqlalchemy.create_engine("mariadb+mariadbconnector://smart_factory_team:0000@172.30.1.11:3306/smart_factory")
    Base = declarative_base()
    Base.metadata.create_all(engine)

if 현재위치 == '망원1층':
    engine = sqlalchemy.create_engine("mariadb+mariadbconnector://smart_factory_team:0000@192.168.200.199:3306/smart_factory")
    Base = declarative_base()
    Base.metadata.create_all(engine)

# ==============================================================================================================
# ==============================================================================================================
# ==============================================================================================================
# ==============================================================================================================

# Create a session
Session = sqlalchemy.orm.sessionmaker()
Session.configure(bind=engine)
session = Session()

# ==============================================================================================================

# 데이터베이스에 있는 날씨 테이블 가져오고, 새로 추가할 시작날짜   무조건 !!!! 시작일자는 월요일, 끝일은 상관없음. 다만 새로 업데이트시, 마지막일자는 제거후 다시 시작일자를 그 마지막 일자의 다음날로 해야됨 (월요일)
#
날씨기존df = pd.read_sql_table('날씨',con=engine)

last_day = max(날씨기존df['일자'])

last_day_year = last_day.year
last_day_month = last_day.month
last_day_day = last_day.day

last_day_year_str = str(last_day_year)

if last_day_month < 10:
    last_day_month_str = '0'+str(last_day_month)
else:
    last_day_month_str = str(last_day_month)

if last_day_day < 10:
    last_day_day_str = '0'+str(last_day_day)
else:
    last_day_day_str = str(last_day_day)

last_day_delete = last_day_year_str + '-' + last_day_month_str + '-'+last_day_day_str


last_day_mon = last_day-timedelta(days=6)    # 현재 table에 있는 마지막 날짜는 일요일. 6일전, 그 전주 월요일 부터 다시 계산
                                               # 이유는, end date을 현재로 한다고 했을때,


year = last_day_mon.year
month = last_day_mon.month
day = last_day_mon.day


year_str = str(year)  #연도 str 변환

if month < 10:
    month_str = '0'+str(month)
else:
    month_str = str(month)

if day < 10:
    day_str = '0'+str(day)
else:
    day_str = str(day)

start_date = year_str+month_str+day_str


# end_date
import datetime
now = datetime.datetime.now()-timedelta(days=1)
year_now =now.year
month_now = now.month
day_now = now.day

year_now_str = str(year_now)  #연도 str 변환

if month_now < 10:
    month_now_str = '0'+str(month_now)
else:
    month_now_str = str(month_now)

if day_now < 10:
    day_now_str = '0'+str(day_now)
else:
    day_now_str = str(day_now)

end_date = year_now_str+month_now_str+day_now_str



### 유훈함수 breaking
import json
import pandas as pd
import requests
import numpy as np
# 내가 한번 쭉 확인 필요

for index, location in enumerate(['서울','경기도북부','경기도남부','인천','부산광역시','대구광역시','울산광역시','경상북도','경상남도','전라북도','전라남도','대전광역시','세종시','충청북도','충청남도','강원도']):
    def cat_location(x):
        if x == '서울':
            return 108
        elif x == '경기도북부':
            return 98
        elif x == '경기도남부':
            return 119
        elif x == '인천':
            return 112
        elif x == '부산광역시':
            return 159
        elif x == '대구광역시':
            return 152
        elif x == '울산광역시':
            return 143
        elif x == '경상북도':
            return 278
        elif x == '경상남도':
            return 263
        elif x == '전라북도':
            return 146
        elif x == '전라남도':
            return 156
        elif x == '대전광역시':
            return 133
        elif x == '세종시':
            return 239
        elif x == '충청북도':
            return 131
        elif x == '충청남도':
            return 129
        elif x == '강원도':
            return 114
        else:
            return 119  ## 해당 안될시 생판지역인 평택 기준 날씨로 변환


    location_code = cat_location(location)

    url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'

    params = {'serviceKey': 'ZKOx0KH7l+PcSZZNRvuI54pjFf5gbYeIa1ccvoUcbzlwPA7ZRd9AqYB+V6++N/urN+9OncLmDH9MvqvMu5SKbg==',
              'pageNo': '1',
              'numOfRows': '999',
              'dataType': 'JSON',
              'dataCd': 'ASOS',
              'dateCd': 'DAY',
              'startDt': start_date,
              'endDt': end_date,
              'stnIds': location_code}

    response = requests.get(url, params=params)
    r_dict = json.loads(response.text)
    r_response = r_dict.get("response")
    r_body = r_response.get("body")
    r_items = r_body.get("items")
    r_item = r_items.get("item")

    # city = [] #도시
    time = []  # 일자
    tem = []  # 온도
    rain = []  # 강수여부
    hum = []  # 습도
    snow = []  # 합계3시간 신적설 ( 3시간동안 내린 양 )

    empty_data = pd.DataFrame()

    for i in range(len(r_item)):
        time.append(r_item[i]['tm'])
        # city.append(r_item[i]['stnNm'])
        tem.append(r_item[i]['avgTa'])
        rain.append(r_item[i]['sumRn'])
        hum.append(r_item[i]['avgRhm'])
        snow.append(r_item[i]['sumDpthFhsc'])

    time = pd.Series(time)
    # city = pd.Series(city)
    tem = pd.Series(tem)
    rain = pd.Series(rain)
    hum = pd.Series(hum)
    snow = pd.Series(snow)

    data = [time, tem, rain, hum, snow]

    df = pd.concat(data, axis=1)

    df.columns = ['일자', '기온', '강수량', '습도', '신적설량']
    df.replace('', 0, inplace=True)

    df['일자'] = pd.to_datetime(df['일자'])
    df['기온'] = round(df['기온'].astype('float32'), 1)
    df['강수량'] = round(df['강수량'].astype('float32'), 1)
    df['습도'] = round(df['습도'].astype('float32'), 1)
    df['신적설량'] = round(df['신적설량'].astype('float32'), 1)

    add_df = df.set_index('일자').resample('w').mean()
    add_df['지역'] = location

    if index==0:
        add_total_df = add_df
        print(location, '추가완료')
    else:
        print(location, '추가완료')
        add_total_df =pd.concat([add_total_df,add_df],axis=0)



# ========================================================================================
# mariadb로 추가전, 전에 있던 마지막 주에 대한 데이터를 업데이트 해줘야하기 때문에, 마지막 일자의 행을 다 지워준다.

##  https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_using_delete_expression.htm
from sqlalchemy.sql.expression import update
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String ,VARCHAR,DATE
meta = MetaData()
날씨 = Table(
   '날씨', meta,
   Column('일자', DATE, primary_key = True),
   Column('지역', VARCHAR,primary_key = True)
)
conn = engine.connect()
stmt = 날씨.delete().where(날씨.c.일자 == last_day_delete)
conn.execute(stmt)


# mariadb로 데이터 추가
def add_row(table_name_to_add_row,df_to_add_row):
    df_to_add_row.to_sql(table_name_to_add_row, con=engine, if_exists='append')
add_row('날씨',add_total_df)
