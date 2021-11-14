### 2021/11/10일 버전 오후2시버전


from datetime import timedelta
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


# 필요한 테이블 불러오기
중장기수주예측 = pd.read_sql_table('중장기수주예측',con=engine)
weather = pd.read_sql_table('날씨',con=engine)
건축착공면적 = pd.read_sql_table('건축착공면적',con=engine)

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor

from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import datetime as dt
def get_today():
  today = dt.datetime.today()
  today_year = today.year
  today_month = today.month
  today_day = today.day
  today_day
  today = dt.datetime(today_year,today_month,today_day)
  return today

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor

from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import joblib

def get_today():
  today = dt.datetime.today()
  today_year = today.year
  today_month = today.month
  today_day = today.day
  today_day
  today = dt.datetime(today_year,today_month,today_day)
  return today

std_date =get_today()

end_date = std_date + relativedelta(days=95)
중장기수주예측 = 중장기수주예측[(중장기수주예측['납기일자']>=std_date) & (중장기수주예측['납기일자']<=end_date)]

df = 중장기수주예측.copy()
df.set_index('납기일자',inplace=True)
df['예측중량'] = np.log1p(df['예측중량'])
df.fillna(0,inplace=True)

건축착공면적['TIME'] = pd.to_datetime(건축착공면적['TIME'],format='%Y%m')
today = datetime.today().strftime('%Y-%m-%d')
today = pd.to_datetime(today,format='%Y-%m-%d')
start_date = today - relativedelta(years=2)

def cat(df):
  df = df[df['TIME']>=start_date]
  df['month'] = df.TIME.dt.month
  df = pd.DataFrame(df.groupby('month')['건축착공면적'].mean())
  df.reset_index(inplace=True)
  return df

건축착공면적 = cat(건축착공면적)

df.reset_index(inplace=True)
df['month'] = df['납기일자'].dt.month
# data['year'] = data['납기일자'].dt.year
df = pd.merge(df, 건축착공면적, on='month', how='left')
df.set_index('납기일자',inplace=True)

df = pd.get_dummies(df)

df.reset_index(inplace=True)

X_test = df.drop(['납기일자','month'], axis =1)

#############################
grid_rf = joblib.load('./py_files/regre_prodsale_prediction/reg_save/grid_rf.joblib.dat')
#############################

pred_rf = grid_rf.predict(X_test)

pred = pd.DataFrame(pred_rf,columns=['예측중량'])
pred['예측중량'] = np.ceil(np.expm1(pred.예측중량))
중장기수주예측 = 중장기수주예측[['납기일자','제품명']]
중장기수주예측.reset_index(drop=True,inplace=True)
생판계획 = pd.concat([중장기수주예측,pred],axis=1)

pd.set_option('display.max_rows',None)
생판계획.sort_values(by='납기일자',ascending=True,inplace=True)
생판계획.reset_index(drop=True,inplace=True)


생판계획 = 생판계획.set_index('납기일자')

replace_table(생판계획,'생판계획예측')     #<<생판계획예측>> 에 대체

print()
print("Maria_DB의 <생판계획예측> 테이블이 업데이트 되었습니다.")
print()




