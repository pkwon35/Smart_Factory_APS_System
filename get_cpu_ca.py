
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base

engine = sqlalchemy.create_engine("mariadb+mariadbconnector://root:0000@127.0.0.1:3306/smart_factory")
Base = declarative_base()
Base.metadata.create_all(engine)

# Create a session
Session = sqlalchemy.orm.sessionmaker()
Session.configure(bind=engine)
session = Session()

import pandas as pd
import requests


import datetime as dt
from dateutil.relativedelta import relativedelta

def get_today():
  today = dt.datetime.today()
  today_year = today.year
  today_month = today.month
  today_day = today.day
  today_day
  today = dt.datetime(today_year,today_month,today_day)
  return today

now = get_today()


year = now.year
month = now.month


if month<10:
  month_str = '0'+str(month)
else:
  month_str = str(month)
str_end_get = str(year) + month_str

search_start_get = now - relativedelta(years=5)
year = search_start_get.year
month = search_start_get.month

if month<10:
  month_str = '0'+str(month)
else:
  month_str = str(month)
str_start_get = str(year) + month_str

print(f'{str_start_get} ~ {str_end_get}')


def korea_bank(col_name, code, item_code1='?', item_code2='?'):
  key = 'BBTRYO7WX8OVQ7IL8XZJ'
  data_start = '1'
  data_end= '1000'
  # code = '080Y111'
  cycle = 'MM'
  search_start = str_start_get
  search_end = str_end_get
  # item_code1 = 'I32AL3'
  # item_code2 = 'I11B'
  url = f'http://ecos.bok.or.kr/api/StatisticSearch/{key}/json/kr/{data_start}/{data_end}/{code}/{cycle}/{search_start}/{search_end}/{item_code1}/{item_code2}'
  res = requests.get(url)
  data = res.json()
  data = data['StatisticSearch']['row']
  df = pd.DataFrame(data).set_index('TIME')
  df = df[['DATA_VALUE']]
  df.rename(columns={df.columns[0]:col_name},inplace=True)
  return df

건축착공면적 = korea_bank('건축착공면적','085Y136','1','I47AB')

def table_df(table_name):
  table = pd.read_sql_table(table_name, con=engine)
  return table

from sqlalchemy import MetaData, Table, Column, VARCHAR, DATE, SMALLINT
def replace_table(replace_with, table_name):
  meta = MetaData()
  table_delete = Table(table_name, meta)
  conn = engine.connect()
  stmt = table_delete.delete()
  conn.execute(stmt)
  replace_with.to_sql(table_name, con=engine, if_exists='append')

def add_to_table(rows_to_add, table_name):
  rows_to_add.to_sql(table_name, con=engine, if_exists='append')

replace_table(건축착공면적,'건축착공면적')

print("Maria_DB의  <건축착공면적> 테이블이 업데이트 되었습니다.")

