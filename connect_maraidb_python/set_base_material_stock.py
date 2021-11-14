
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd



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

############################################################

생산레시피기본 =table_df('생산레시피기본')

제품기본 =table_df('제품기본')

자재발주기본 =table_df('자재발주기본')

import re
import numpy as np
import pandas as pd
pd.options.display.max_columns=100


sales = pd.merge(생산레시피기본,제품기본[['제품코드','제품명']],
                 on='제품코드')

def prod_name(제품명):
    prod_name = re.search('[a-zA-Z]+-*[a-zA-Z]*[\d]*[a-zA-Z]*',제품명).group()
    return prod_name

sales['prod_name'] = sales['제품명'].apply(prod_name)

for n,prod in enumerate(sales['prod_name'].unique()):
  원자재 = pd.pivot_table(sales.query(f'prod_name=="{prod}"'),
              index=['LOT번호','제품코드','생산작업요청일자'],
              columns='원자재명',
              values='투입지시비율')

  rows = 원자재.values.tolist()
  recipe = {}
  for row in rows:
    rec = re.sub(',','/',str(row)).strip('[]')
    try:
      recipe[rec]
      recipe[rec] = recipe[rec]+1
    except:
      recipe[rec] = 1

  def get_key(val):
      for key, value in recipe.items():
          if val == value:
              return key
  recipe_df = pd.DataFrame(get_key(max(recipe.values())).strip( ).split('/'),columns=[prod]).T
  recipe_df.columns=원자재.columns
  # recipe_df.replace({'nan':np.nan},inplace=True)
  if n==0:
    recipe_total = recipe_df.copy()
  else:
    recipe_total = pd.concat([recipe_total, recipe_df],axis=0)

recipe_total = recipe_total.dropna(how='all',axis=1)
recipe_total.replace({'nan':np.nan},inplace=True)
recipe_total.replace({' nan':np.nan},inplace=True)
recipe_total = recipe_total.dropna(how='all',axis=1)

원자재코드 = pd.merge(자재발주기본[['원자재코드','입고완료여부']].drop_duplicates(),생산레시피기본[['원자재코드','원자재명']].drop_duplicates(),on='원자재코드').set_index('원자재명').drop('입고완료여부',axis=1)
recipe1 = recipe_total.T.join(원자재코드)
제품_원자재 = recipe1[recipe1['원자재코드'].isnull()].index.tolist()
sales[sales['제품명'].str.contains('PEMA-HR1500우성')]['prod_name'].unique()

recipe_total.fillna(0,inplace=True)
recipe_total = recipe_total.astype('float')
recipe_total.loc['PEMA-HR1500F'] = recipe_total.loc['PEMA-HR1500F']+recipe_total.loc['PEMA-HR1500']*0.25
recipe_total.drop('PEMA-HR1500 우성',axis=1,inplace=True)

for column in recipe_total.columns:
  recipe_total[column] = recipe_total[column]*0.01

recipe_total = recipe_total.reset_index().rename(columns = {'index':'제품명'})
recipe_total = recipe_total.rename_axis(None, axis=1)

add_to_table(recipe_total.set_index('제품명'),'제품별기준투입량')

meta = MetaData()

제품별기준투입량= Table('제품별기준투입량', meta, Column("원자재명", VARCHAR))
meta.create_all(engine)


print("Maria_DB의 <제품별기준투입량> 테이블이 업데이트 되었습니다.")