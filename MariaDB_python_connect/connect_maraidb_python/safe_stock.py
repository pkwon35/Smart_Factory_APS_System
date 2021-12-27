from datetime import timedelta
from dateutil.relativedelta import relativedelta
from datetime import datetime
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

#### SQLALCHEMY 사용자함수
from sqlalchemy import MetaData, Table, Column, VARCHAR, DATE, SMALLINT

def replace_table(replace_with,table_name):
    meta = MetaData()
    table_delete = Table(table_name, meta)
    conn = engine.connect()
    stmt = table_delete.delete()
    conn.execute(stmt)
    replace_with.to_sql(table_name, con=engine, if_exists='append')
def add_to_table(rows_to_add,table_name):
    rows_to_add.to_sql(table_name, con=engine, if_exists='append')

import datetime as dt
from dateutil.relativedelta import relativedelta
from datetime import timedelta
def get_today():
  today = dt.datetime.today()
  today_year = today.year
  today_month = today.month
  today_day = today.day
  today_day
  today = dt.datetime(today_year,today_month,today_day)
  return today
# @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ #
now = get_today()


제품별기준투입량 =pd.read_sql_table('제품별기준투입량',con=engine)
수주분석테이블 =pd.read_sql_table('수주분석테이블',con=engine)


import pandas as pd

# @@@@@@@@@@@@@@@@ 한달에 한번 실행 @@@@@@@@@@@@@@@@

##필요테이블
#### 안전재고량은 지난 3년간 사용된 원자료만을 대상으로 진행
## 원래는 영업수주기본에서 데이터 가져와야하지만, 일단 증식데이터를 영업수주기본  취급....

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

maxleadtime = 6
avgleadtime = 6

## 현시점부터 과걱 3년치 수주데이터를 가지고와, 각 일자별 원자재 사용량 확인 후 "일일최고판매량" , "일일평균판매량"  구해서 안전 재고 수량 계산
# 안전재고량 식 적용   (일일 최고 판매량 x 최대 리드 타임 (일)) - (일일 평균 판매량 x 평균 리드 타임 (일)) = 안전 재고 수량


last_date = today - timedelta(days=1)

start_date = last_date - relativedelta(years=3)  # 현시점부터 과거 3년치 수주데이터를 가져와 원자재 일별 평균/최대 사용량 찾기

영업수주기본_check = 수주분석테이블[
  (수주분석테이블['납기일자'] >= start_date) & (수주분석테이블['납기일자'] <= last_date)]

안전재고량_check = 영업수주기본_check.groupby(['납기일자', '제품명'])[['판매수량']].sum()
안전재고량_check = 안전재고량_check.reset_index()
안전재고량_check

원자재_사용량 = pd.DataFrame(columns=['원자재사용일자', '원자재명', '원자재사용량'])
import numpy as np
from tqdm import tqdm

days_diff = (last_date - start_date).days
for addday in tqdm(range(days_diff + 1)):
  check_date = start_date + timedelta(days=addday)  # 수주데이터 현시점 3년전부터 하루하루 제품별 확인해 일별 원재료 사용량 뽑기
  안전재고량_check_일별 = 안전재고량_check[안전재고량_check['납기일자'] == check_date]
  사용량합치기 = pd.merge(안전재고량_check_일별, 제품별기준투입량, on='제품명', how='left')
  for 원자재 in 사용량합치기.columns[3:]:
    사용량합치기[원자재] = 사용량합치기[원자재] * 사용량합치기['판매수량']
  원자재별_사용량 = pd.DataFrame(columns=['원자재사용일자', '원자재명', '원자재사용량'])  # 특정일자에 대한 원자재별 사용량 붙이는 df

  for 원자재 in 사용량합치기.columns[3:]:  # 원자재별 총량 구하기
    원자재사용량 = float(사용량합치기[원자재].sum())
    추가 = pd.DataFrame(np.array([[check_date, 원자재, 원자재사용량]]), columns=['원자재사용일자', '원자재명', '원자재사용량'])
    원자재별_사용량 = pd.concat([원자재별_사용량, 추가], axis=0)

  원자재_사용량 = pd.concat([원자재_사용량, 원자재별_사용량], axis=0)
원자재_사용량 = 원자재_사용량.set_index('원자재사용일자')
원자재_사용량['원자재사용량'] = 원자재_사용량['원자재사용량'].astype('float')

# 안전재고량 식 적용   (일일 최고 판매량 x 최대 리드 타임 (일)) - (일일 평균 판매량 x 평균 리드 타임 (일)) = 안전 재고 수량

안전재고량기준 = pd.DataFrame(columns=['원자재명', '일일최고판매량', '일일평균판매량'])
for 원자재명 in 원자재_사용량['원자재명'].unique():
  check_원자재 = 원자재_사용량[원자재_사용량['원자재명'] == 원자재명]
  원자재평균 = check_원자재['원자재사용량'].mean()
  원자재max = check_원자재['원자재사용량'].max()

  안전재고량기준_추가 = pd.DataFrame(np.array([[원자재명, 원자재max, 원자재평균]]), columns=['원자재명', '일일최고판매량', '일일평균판매량'])
  안전재고량기준 = pd.concat([안전재고량기준, 안전재고량기준_추가], axis=0)
안전재고량기준['maxleadtime'] = maxleadtime
안전재고량기준['avgleadtime'] = avgleadtime
안전재고량기준['일일최고판매량'] = 안전재고량기준['일일최고판매량'].astype('float')
안전재고량기준['일일평균판매량'] = 안전재고량기준['일일평균판매량'].astype('float')

max = (안전재고량기준['일일최고판매량'] * 안전재고량기준['maxleadtime'])
avg = (안전재고량기준['일일평균판매량'] * 안전재고량기준['avgleadtime'])

안전재고량기준['안전재고량'] = max - avg
안전재고량기준['안전재고량'] = np.ceil(안전재고량기준['안전재고량'].astype('float'))
안전재고량기준 = 안전재고량기준.set_index('원자재명')
안전재고량기준 = 안전재고량기준[['안전재고량']]


replace_table(안전재고량기준, '안전재고량기준')


print("Maria_DB의 <안전재고량기준> 테이블이 업데이트 되었습니다.")