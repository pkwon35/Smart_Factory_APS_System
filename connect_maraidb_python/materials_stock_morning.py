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

def add_row(table_name_to_add_row,df_to_add_row):
    df_to_add_row.to_sql(table_name_to_add_row, con=engine, if_exists='append')

수주분석테이블 = table_df('수주분석테이블')

생판계획예측 =table_df('생판계획예측')

제품별기준투입량 =table_df('제품별기준투입량')

안전재고량기준 =table_df('안전재고량기준')

원자재재고량 = table_df('원자재재고량')

# 원자재자동발주내역 = table_df('원자재자동발주내역')

## 하루에 한번 오전 9시에 실행
## 테이블 <제품별_기준_원자재투입량>
원자재재고량
생판계획예측
###################################

import datetime as dt
from dateutil.relativedelta import relativedelta
from datetime import timedelta
import numpy as np
def get_today():
  today = dt.datetime.today()
  today_year = today.year
  today_month = today.month
  today_day = today.day
  today = dt.datetime(today_year,today_month,today_day)
  return today

now = get_today()

## 확인할 납기일자를 뽑기위해 날짜가져오기



# 오늘날짜를 기준으로 발주가 들어간다. 발주량 기준은, 현재부터 9일뒤 납품예정인 제품에 대한 생판예측량

start_orderdate = now                               #오늘 발주날짜
start_pred = now + timedelta(days=9)                # 오늘발주기준 생판예상날짜  (오늘부터 9일뒤 생판예측확인)
last_pred =  생판계획예측['납기일자'].max()         # 마지막 생판예상날짜
last_orderdate = last_pred - timedelta(days=9)      # 마지막 생판예측에 대한 발주해야되는 날짜
days_diff = (last_orderdate - start_orderdate).days
원자재필요량 = pd.DataFrame(columns = ['원자재주문일자','원자재명','원자재필요량'])
for addday in range(days_diff-1):
  check_pred_date =start_pred+timedelta(days=addday)    # 발주대상 첫 날짜부터 생판예측된 마지막 날짜까지 확인
  check_order_date = check_pred_date - timedelta(days=9)
  생판계획예측check=생판계획예측[생판계획예측['납기일자']==check_pred_date]

  사용량합치기 = pd.merge(생판계획예측check,제품별기준투입량,on='제품명',how='left')
  사용량합치기
  for 원자재 in 사용량합치기.columns[3:]:
      사용량합치기[원자재] =사용량합치기[원자재]*사용량합치기['예측중량']

  원자재별_발주량 = pd.DataFrame(columns = ['원자재주문일자','원자재명','원자재필요량'])   # 특정일자에 대한 원자재별 주문량 붙이는 df

  for 원자재 in 사용량합치기.columns[3:]:     #원자재별 총량 구하기
      원자재필요량sum = 사용량합치기[원자재].sum()
      추가 = pd.DataFrame(np.array([[check_order_date, 원자재,원자재필요량sum]]),columns =['원자재주문일자','원자재명','원자재필요량'])
      원자재별_발주량 = pd.concat([원자재별_발주량,추가],axis=0)
  원자재필요량 = pd.concat([원자재필요량,원자재별_발주량],axis=0)
원자재필요량['원자재필요량'] = 원자재필요량['원자재필요량'].astype('float')

# 생판계획예측에서 뽑아낸 각 생산일자별 원자재별 필요량 산출
생판계획예측원재료량 = 원자재필요량

생판계획예측원재료량['생산일자'] = 생판계획예측원재료량['원자재주문일자'] + timedelta(days=6)

생판계획예측원재료량 = 생판계획예측원재료량[['생산일자','원자재명','원자재필요량']]

생판계획예측원재료량 = 생판계획예측원재료량.rename(columns = {"원자재필요량":"예측필요중량"})

생판계획예측원재료량 = 생판계획예측원재료량.set_index('생산일자')

replace_table(생판계획예측원재료량,'생판계획예측원재료량')
print("Maria_DB의 <생판계획예측원재료량> 테이블이 업데이트 되었습니다.")
print()

# 일별 구해진 원자재 필요량에서 오늘날짜에 대해서만 가져오기

원자재필요량today = 원자재필요량[원자재필요량['원자재주문일자'] == now]

원자재주문량 = pd.merge(원자재재고량,원자재필요량today[['원자재명','원자재필요량']],on='원자재명',how='left')

원자재주문량['원자재주문량'] = abs(np.ceil(원자재주문량['원자재필요량'] - 원자재주문량['안전재고량상태']))   #ceil 처리함으로써, 너무 작은 소수점은 올려준다

원자재주문량['안전재고량상태'] = 0.0

원자재자동발주내역 = 원자재주문량[['원자재명','원자재주문량']]

원자재자동발주내역['발주일자'] = now

원자재자동발주내역 = 원자재자동발주내역.set_index('원자재명')

add_row('원자재자동발주내역',원자재자동발주내역)
print("Maria_DB의 <원자재자동발주내역> 테이블이 업데이트 되었습니다.")

print("<< 자동발주실행 >>")
# 위 원자재주문량 가지고 자동발주 실시되었다고 가정하고, 원자재재고량 변동주기

원자재재고량수정 = pd.merge(원자재재고량,원자재주문량[['원자재명','원자재주문량']],on='원자재명',how='left')

원자재재고량수정['원자재재고량'] = 원자재재고량수정['원자재재고량'] + 원자재재고량수정['원자재주문량']

원자재재고량수정['안전재고량상태'] = 원자재재고량수정['원자재재고량'] - 원자재재고량수정['안전재고량']

원자재재고량오전= 원자재재고량수정.drop('원자재주문량',axis=1)

원자재재고량오전 = 원자재재고량오전.set_index('원자재명')   #4월5일에 대한 원자재 주문이 완료된 후 재고량       -->  재고량 테이블 업데이트

replace_table(원자재재고량오전, '원자재재고량')

print("Maria_DB의 <원자재재고량> 테이블이 오전 기준 발주 후 업데이트 되었습니다.")