from datetime import timedelta
from dateutil.relativedelta import relativedelta

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

now = get_today()


영업수주기본 = pd.read_sql_table('영업수주기본',con=engine)

##
# 영업수주기본 table에서 필요 컬럼만 뽑고, 필요한 기간 (현시점 부터 4년전, 즉 4월1일부터 4년전)
##

# 현재 모델링 돌아가는 시점까지의 실데이터 가져오기

enddate = now-timedelta(days=1)
startdate = now - relativedelta(years=4)

print(f"시작 : {startdate}\n끝 : {enddate}")

수주from = 영업수주기본[(영업수주기본['납기일자']>=startdate)][['거래처코드','제품코드','납기일자','판매수량']]

수주from_to = 수주from[수주from['납기일자']<=now+timedelta(days=3)].sort_values(by='납기일자')

제품기본 = pd.read_sql_table('제품기본',con=engine)

수주분석테이블 = pd.merge(수주from_to,제품기본[['제품코드','제품명']],on='제품코드')

import re
import numpy as np
def prod_name(제품명):
    prod_name = re.search('[a-zA-Z]+-*[a-zA-Z]*[\d]*[a-zA-Z]*',제품명).group()
    return prod_name

def corp(제품명):
    corp = re.search('[가-힣]+',제품명).group()
    return corp

def region(제품명):
    try:
        region = re.search(r'\([가-힣]+\)',제품명).group().strip('()')
        return region
    except:
        return np.nan

# 수주분석테이블['corp'] = 수주분석테이블['제품명'].apply(corp)
수주분석테이블['지역'] = 수주분석테이블['제품명'].apply(region)
수주분석테이블['제품명'] = 수주분석테이블['제품명'].apply(prod_name)
수주분석테이블.set_index('거래처코드',inplace=True)

####################### mariadb 테이블 (컬럼 타입은 유지하고) replace
from sqlalchemy import  MetaData, Table, Column,VARCHAR,DATE,SMALLINT

meta = MetaData()

수주분석테이블_delete = Table(
   '수주분석테이블', meta
)
conn = engine.connect()
stmt = 수주분석테이블_delete.delete()
conn.execute(stmt)

def replace_table(table_name_to_add_row,df_to_add_row):
    df_to_add_row.to_sql(table_name_to_add_row, con=engine, if_exists='append')
replace_table('수주분석테이블',수주분석테이블)


print("Maria_DB의 <수주분석테이블> 테이블이 업데이트 되었습니다.")