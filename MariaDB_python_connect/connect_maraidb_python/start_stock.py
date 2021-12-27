### 처음 APS 솔루션 도입시, 기준 원자재재고량을 설정 후 진행.



from datetime import timedelta
import inline as inline
import matplotlib
from dateutil.relativedelta import relativedelta
from datetime import datetime
import numpy as np
import sqlalchemy
import pandas as pd
from sqlalchemy.ext.declarative import declarative_base

engine = sqlalchemy.create_engine("mariadb+mariadbconnector://root:0000@127.0.0.1:3306/smart_factory")
Base = declarative_base()
Base.metadata.create_all(engine)

# Create a session
Session = sqlalchemy.orm.sessionmaker()
Session.configure(bind=engine)
session = Session()
from sqlalchemy import MetaData, Table, Column, VARCHAR, DATE, SMALLINT
def table_df(table_name):
    table = pd.read_sql_table(table_name, con=engine)
    return table
def replace_table(replace_with,table_name):
    meta = MetaData()
    table_delete = Table(table_name, meta)
    conn = engine.connect()
    stmt = table_delete.delete()
    conn.execute(stmt)
    replace_with.to_sql(table_name, con=engine, if_exists='append')

def add_to_table(rows_to_add,table_name):
    rows_to_add.to_sql(table_name, con=engine, if_exists='append')


# ===================================================================================

### 원자재의 종류수가 늘어날 수 있기때문에, 테이블 자체를 날려준다.
meta = MetaData()
table_delete = Table('원자재자동발주내역', meta)
conn = engine.connect()
stmt = table_delete.delete()
conn.execute(stmt)


import pandas as pd


##### 필요한 테이블
수주분석테이블 = table_df('수주분석테이블')

생판계획예측 =table_df('생판계획예측')

제품별기준투입량 =table_df('제품별기준투입량')

안전재고량기준 =table_df('안전재고량기준')

import datetime as dt
from dateutil.relativedelta import relativedelta
from datetime import timedelta


def get_today():
    today = dt.datetime.today()
    today_year = today.year
    today_month = today.month
    today_day = today.day
    today = dt.datetime(today_year, today_month, today_day)
    return today


today = get_today()


get_until = today + timedelta(days=8)  # 현재부터 8일후까지에 대한 납기일자 원자재 주문한 것 뽑기위해
period = '납기일자'
amount = '예측중량'
df = 생판계획예측

df_check = df[(df[period] >= today) & (df[period] <= get_until)]

dfg_check = df_check.groupby([period, '제품명'])[[amount]].sum()
dfg_check = dfg_check.reset_index()

원자재_사용량 = pd.DataFrame(columns=['원자재사용일자', '원자재명', '원자재사용량'])
import numpy as np
from tqdm import tqdm

days_diff = (get_until - today).days
for addday in tqdm(range(days_diff + 1)):
    check_date = today + timedelta(days=addday)  # 수주데이터 현시점 3년전부터 하루하루 제품별 확인해 일별 원재료 사용량 뽑기
    dfg_check_일별 = dfg_check[dfg_check[period] == check_date]
    사용량합치기 = pd.merge(dfg_check_일별, 제품별기준투입량, on='제품명', how='left')
    for 원자재 in 사용량합치기.columns[3:]:
        사용량합치기[원자재] = 사용량합치기[원자재] * 사용량합치기[amount]
    원자재별_사용량 = pd.DataFrame(columns=['원자재사용일자', '원자재명', '원자재사용량'])  # 특정일자에 대한 원자재별 사용량 붙이는 df

    for 원자재 in 사용량합치기.columns[3:]:  # 원자재별 총량 구하기
        원자재사용량 = float(사용량합치기[원자재].sum())
        추가 = pd.DataFrame(np.array([[check_date, 원자재, 원자재사용량]]), columns=['원자재사용일자', '원자재명', '원자재사용량'])
        원자재별_사용량 = pd.concat([원자재별_사용량, 추가], axis=0)

    원자재_사용량 = pd.concat([원자재_사용량, 원자재별_사용량], axis=0)

원자재_사용량['원자재사용량'] = 원자재_사용량['원자재사용량'].astype('float')
주문완료재고원자재 = 원자재_사용량.groupby('원자재명')[['원자재사용량']].sum()
주문완료재고원자재 = 주문완료재고원자재.reset_index()
주문완료재고원자재 = 주문완료재고원자재.rename(columns={'원자재사용량': '생판예측주문완료재고'})
주문완료재고원자재  ## 오늘 ~ 8일후 생산납품될 제품에대한 이미 주문이 된 원재료량

# 안전재고량 < 안전재고량기준 >
안전재고량기준 = 안전재고량기준.reset_index()
# 4월5일 ~ 4월7일 납기일자에 대한 제품량으로 각각 일자별 원재료량 구하기 < 영업수주기본 >
수주분석테이블
수주분석테이블_check = 수주분석테이블[(수주분석테이블['납기일자'] >= today) & (수주분석테이블['납기일자'] <= today + timedelta(days=2))][
    ['납기일자', '제품명', '판매수량']]
수주분석테이블_check.sort_values(by='납기일자')

get_until = today + timedelta(days=2)
period = '납기일자'
amount = '판매수량'
df = 수주분석테이블

df_check = df[(df[period] >= today) & (df[period] <= get_until)]

dfg_check = df_check.groupby([period, '제품명'])[[amount]].sum()
dfg_check = dfg_check.reset_index()

원자재_사용량 = pd.DataFrame(columns=['원자재사용일자', '원자재명', '원자재사용량'])
import numpy as np
from tqdm import tqdm

days_diff = (get_until - today).days
for addday in tqdm(range(days_diff + 1)):
    check_date = today + timedelta(days=addday)  # 수주데이터 현시점 3년전부터 하루하루 제품별 확인해 일별 원재료 사용량 뽑기
    dfg_check_일별 = dfg_check[dfg_check[period] == check_date]
    사용량합치기 = pd.merge(dfg_check_일별, 제품별기준투입량, on='제품명', how='left')
    for 원자재 in 사용량합치기.columns[3:]:
        사용량합치기[원자재] = 사용량합치기[원자재] * 사용량합치기[amount]
    원자재별_사용량 = pd.DataFrame(columns=['원자재사용일자', '원자재명', '원자재사용량'])  # 특정일자에 대한 원자재별 사용량 붙이는 df

    for 원자재 in 사용량합치기.columns[3:]:  # 원자재별 총량 구하기
        원자재사용량 = float(사용량합치기[원자재].sum())
        추가 = pd.DataFrame(np.array([[check_date, 원자재, 원자재사용량]]), columns=['원자재사용일자', '원자재명', '원자재사용량'])
        원자재별_사용량 = pd.concat([원자재별_사용량, 추가], axis=0)

    원자재_사용량 = pd.concat([원자재_사용량, 원자재별_사용량], axis=0)

원자재_사용량['원자재사용량'] = 원자재_사용량['원자재사용량'].astype('float')
생산투입원자재 = 원자재_사용량.groupby('원자재명')[['원자재사용량']].sum()
생산투입원자재 = 생산투입원자재.reset_index()
생산투입원자재 = 생산투입원자재.rename(columns={'원자재사용량': '생산투입원자재'})
생산투입원자재  ## 오늘 ~ 2일후 생산납품될 제품에대한 이미 주문이 된 원재료량

# merge

기존재고량 = pd.merge(안전재고량기준,주문완료재고원자재,on='원자재명',how='left')

기존재고량 = pd.merge(기존재고량,생산투입원자재,on='원자재명',how='left')

기존재고량['원자재재고량'] = 기존재고량['안전재고량'] + 기존재고량['생판예측주문완료재고'] - 기존재고량['생산투입원자재']

기존재고량['원자재재고량'] =np.ceil(기존재고량['원자재재고량'])

기존재고량['안전재고량상태'] = 기존재고량['원자재재고량'] - 기존재고량['안전재고량']


#
# def 안전재고량_부족(x):
#   if x>0:
#     return 0
#   else:
#     return x
#
# 기존재고량['안전재고량상태'] = 기존재고량['안전재고량상태'].apply(안전재고량_부족)



기존재고량 = 기존재고량[['원자재명','안전재고량','원자재재고량','안전재고량상태']]

기존재고량 = 기존재고량.set_index('원자재명')


###  처음 모델 설정시에만 실행. 기존재고량은      원자재재고량 테이블로 생성.

replace_table(기존재고량, '원자재재고량')

print("Maria_DB의 기존재고량을 산출 후, <원자재재고량> 테이블을 생성하였습니다.")

