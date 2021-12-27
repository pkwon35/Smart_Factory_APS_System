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

실제 = pd.read_sql_table('수주분석테이블',con=engine)

# 실제데이터는 현재 2018년1월1일 부터 쭉 있다. 하지만, 시계열 분석시에는, 현 시점부터, 2년과거 데이터만, 즉 현재가 4월1일 2021년 이라는 가정하게, 4월1일 2018 년 부터만 가져와시각화.


enddate = now-timedelta(days=1)

startdate = now - relativedelta(years=2)

print(f"<< 실제 {startdate} ~ {enddate} >>")

실제from = 실제[(실제['납기일자']>=startdate)]

실제 = 실제from[실제from['납기일자']<=enddate].sort_values(by='납기일자')

예측 = pd.read_sql_table('중장기수주예측',con=engine)

예측 = 예측[예측['납기일자']>=now]

enddatepred = 예측['납기일자'].max()

print(f"<< 예측 {now} ~ {enddatepred} >>")


중장기실제 = 실제[['납기일자','판매수량','제품명']]


중장기실제 = 중장기실제.groupby(['납기일자','제품명']).sum().reset_index(['납기일자','제품명'])

제품명들 = list(중장기실제['제품명'].unique())

중장기예측 = 예측.rename(columns={'예측중량':'판매수량'})


중장기실제 = 중장기실제[['납기일자','판매수량','제품명']]

중장기실제['is_pred']=0
중장기예측['is_pred']=1

중장기시각화 = pd.concat([중장기실제,중장기예측],axis=0)
중장기시각화 = 중장기시각화.sort_values(by=['납기일자','제품명'])

중장기시각화.set_index('납기일자',inplace=True)

from sqlalchemy import  MetaData, Table, Column,VARCHAR,DATE,SMALLINT
meta = MetaData()

def replace_table(replace_with,table_name):
    meta = MetaData()
    table_delete = Table(table_name, meta)
    conn = engine.connect()
    stmt = table_delete.delete()
    conn.execute(stmt)
    replace_with.to_sql(table_name, con=engine, if_exists='append')


replace_table(중장기시각화,'중장기시각화')
print()
print("Maria_DB의 <중장기시각화> 테이블이 업데이트 되었습니다.")