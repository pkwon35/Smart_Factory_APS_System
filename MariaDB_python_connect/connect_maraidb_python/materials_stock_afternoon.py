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


원자재재고량 = table_df('원자재재고량')
수주분석테이블 = table_df('수주분석테이블')


제품별기준투입량 =table_df('제품별기준투입량')
# 안전재고량기준 =table_df('안전재고량기준')

# 원자재자동발주내역 = table_df('원자재재고량')


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


### 수주분석테이블 테이블에서 4월8일의 생산수주   오늘부터+3일

수주분석테이블_check =수주분석테이블[수주분석테이블['납기일자']==(now+timedelta(days=3))][['납기일자','제품명','판매수량']]
수주분석테이블_check.sort_values(by = '납기일자')
수주분석테이블_check

period = '납기일자'
amount = '판매수량'
df = 수주분석테이블_check

dfg_check = df.groupby('제품명')[[amount]].sum()
dfg_check_일별 = dfg_check.reset_index()

원자재_사용량 = pd.DataFrame(columns = ['원자재사용일자','원자재명','원자재사용량'])
import numpy as np
from tqdm import tqdm


check_date =now+timedelta(days=3)    # 현시점 삼일뒤 납기에 대한 정보가 수주분석테이블에 업데이트 되었을 것이라 가정하고

사용량합치기 = pd.merge(dfg_check_일별,제품별기준투입량,on='제품명',how='left')
for 원자재 in 사용량합치기.columns[3:]:
      사용량합치기[원자재] =사용량합치기[원자재]*사용량합치기[amount]
원자재별_사용량 = pd.DataFrame(columns = ['원자재사용일자','원자재명','원자재사용량'])   # 특정일자에 대한 원자재별 사용량 붙이는 df

for 원자재 in 사용량합치기.columns[3:]:     #원자재별 총량 구하기
    원자재사용량 = float(사용량합치기[원자재].sum())
    추가 = pd.DataFrame(np.array([[check_date, 원자재,원자재사용량]]),columns =['원자재사용일자','원자재명','원자재사용량'])
    원자재별_사용량 = pd.concat([원자재별_사용량,추가],axis=0)

원자재_사용량 = pd.concat([원자재_사용량,원자재별_사용량],axis=0)

원자재_사용량['원자재사용량'] = 원자재_사용량['원자재사용량'].astype('float')
생산투입원자재 = 원자재_사용량.groupby('원자재명')[['원자재사용량']].sum()
생산투입원자재 = 생산투입원자재.reset_index()
생산투입원자재 = 생산투입원자재.rename(columns = {'원자재사용량':'생산투입량'})
생산투입원자재       ## 오늘부터 3일뒤의 생산주문이 들어왔다 가정하고 빼주기

일말재고량 = pd.merge(원자재재고량,생산투입원자재,on='원자재명',how='left')
일말재고량['생산투입량'] =일말재고량['생산투입량'].fillna(0)
일말재고량['원자재재고량'] = 일말재고량['원자재재고량'] - 일말재고량['생산투입량']

일말재고량['안전재고량상태'] = 일말재고량['원자재재고량'] - 일말재고량['안전재고량']


일말재고량 =일말재고량 [['원자재명','안전재고량','원자재재고량','안전재고량상태']]


일말재고량   ### 원자재재고량 테이블 변경

일말재고량  = 일말재고량.set_index('원자재명')



replace_table(일말재고량, '원자재재고량')


print("Maria_DB의 <원자재재고량> 테이블이 금일 수주내역 확인 후 업데이트 되었습니다.")