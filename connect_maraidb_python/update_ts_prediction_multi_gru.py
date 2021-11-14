from datetime import timedelta
import inline as inline
import matplotlib
from dateutil.relativedelta import relativedelta
from datetime import datetime
from datetime import timedelta
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
def get_today():
  today = datetime.datetime.today()
  today_year = today.year
  today_month = today.month
  today_day = today.day
  today_day
  today = datetime.datetime(today_year,today_month,today_day)
  return today
def date_to_str_dash(date):
    year = date.year
    month = date.month
    day = date.day
    year_str = str(year)

    #월가져오기
    if month<10:
        month_str = '0' + str(month)
    else:
        month_str = str(month)
    if day<10:
        day_str = '0' + str(day)
    else:
        day_str = str(day)
    return year_str +'-'+ month_str+'-' + day_str

############ 필요테이블  ############
수주분석테이블 = pd.read_sql_table('수주분석테이블',con=engine)
날씨 =pd.read_sql_table('날씨',con=engine)
print('수주분석테이블을 불러옵니다')
###################################


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler, StandardScaler
import warnings
warnings.filterwarnings('ignore')
from scipy import stats

import warnings
warnings.filterwarnings('ignore')
import datetime
from dateutil.relativedelta import relativedelta
from keras.models import load_model
from tqdm import tqdm

temp_data=날씨
temp_data = temp_data[temp_data['지역'] == '대전광역시']
temp_data.rename(columns={'일자': '납기일자'}, inplace=True)

product_list = 수주분석테이블['제품명'].value_counts().index

add_to_table_final= pd.DataFrame()


now = get_today()
date_start = now - timedelta(days=935)
date_until = now-timedelta(days=1)

date_start_str =date_to_str_dash(date_start)
date_until_str =date_to_str_dash(date_until)

for prod_index in tqdm(range(len(product_list))):
    print(product_list[prod_index])
    #### 데이터 전처리 ####
    org_df = 수주분석테이블



    org_df = org_df[org_df['납기일자'] > date_start_str]
    # 상품선택
    prod0_df = org_df[org_df['제품명'] == product_list[prod_index]]
    prod0_df = prod0_df.groupby('납기일자')[['판매수량']].sum()
    prod0_df.sort_values('납기일자')

    ## 진입데이터
    data = pd.merge(prod0_df, temp_data, on='납기일자')[['납기일자', '판매수량', '기온', '습도']]
    data = data.set_index('납기일자')
    data = data.sort_values('납기일자')


    train_dates = data.index

    # scaring
    df_for_training = data
    cols = list(data.columns)

    from sklearn.preprocessing import MinMaxScaler

    scaler = MinMaxScaler()
    # 스케일을 적용할 column을 정의합니다.
    scale_cols = ['판매수량', '기온', '습도']
    # 스케일 후 columns
    df_for_training_scaled = scaler.fit_transform(df_for_training)
    df_for_training_scaled

    trainX = []
    trainY = []

    n_future = 2  #
    n_past = int(len(train_dates) * 0.1)

    if n_past == 0:
        n_past = 1

    # print(n_past)
    for i in range(n_past, len(df_for_training_scaled) - n_future + 1):
        trainX.append(df_for_training_scaled[i - n_past:i, 0:df_for_training.shape[1]])
        trainY.append(df_for_training_scaled[i + n_future - 1:i + n_future, 0])

    trainX, trainY = np.array(trainX), np.array(trainY)

    # freq 조정해서 6개월에 대한 날짜 맞춰야함
    n_future = 180  # 예측기간

    forecast_period_dates = pd.date_range(date_until_str, periods=n_future, freq='1d').tolist()  # 기간동안의 datelist

    # 예측값 inverse_scaled
    from keras.models import load_model

    my_GRU_model = load_model(f'./py_files/long_ts_prediction/multi_gru/{product_list[prod_index]}.h5')
    # my_GRU_model = load_model(f'./long_ts_prediction/multi_gru/{product_list[prod_index]}.h5')
    try:
        forecast = my_GRU_model.predict(trainX[-n_future:])
        forecast_copies = np.repeat(forecast, df_for_training.shape[1],
                                    axis=-1)  # np.repeat(a, n) a를 n개 복사, axis=-1 현재 배열의 마지막 axis
        y_pred_future = scaler.inverse_transform(forecast_copies)[:, 0]

        ## predict 반환하는 dataframe 만들어주기
        forecast_dates = []
        for time_i in forecast_period_dates:
            forecast_dates.append(time_i.date())

        len_forecast = len(forecast[:, -1])
        # forecast_dates = pd.date_range(forecast_dates[1], periods=n_future, freq = str(int(n_future/len_forecast))+'d').tolist() # <= 날짜 길이 맞지않는거 대비해  [ 예측요청일(180) / 예측값나온일 ] 해서 frequency로 데이터프레임 날짜 들어가게 띄어주기
        df_forecast = pd.DataFrame({'납기일자': pd.date_range(forecast_period_dates[1], periods=len_forecast,
                                                          freq=str(int(n_future / len_forecast)) + 'd').tolist(),
                                    '예측중량': y_pred_future})
        df_forecast['제품명'] = product_list[prod_index]
        df_forecast['납기일자'] = pd.to_datetime(df_forecast['납기일자'])

        # con_data = pd.concat([prod0_df, df_forecast])[['납기일자', '예측중량']]  # 전체기간~예측기간 까지의 데이터
        # con_data.reset_index(inplace=True, drop=True)
        # con_data
        add_to_table_final = pd.concat([add_to_table_final, df_forecast])
    except:

        BATCH_SIZE = 16
        time_steps = n_past
        unit = 64
        from keras.models import Sequential
        from keras.layers import Dense, SimpleRNN, GRU
        from tensorflow.keras.losses import Huber
        # import os
        # os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

        from keras.layers import Dropout

        # The GRU architecture
        my_GRU_model = Sequential()
        # First GRU layer with Dropout regularisation
        my_GRU_model.add(GRU(units=unit, return_sequences=True, activation='tanh'))
        my_GRU_model.add(Dropout(0.2))
        # Second GRU layer
        my_GRU_model.add(GRU(units=unit, return_sequences=True, activation='tanh'))
        my_GRU_model.add(Dropout(0.2))

        # Third GRU layer
        my_GRU_model.add(GRU(units=unit, return_sequences=True, activation='tanh'))
        my_GRU_model.add(Dropout(0.2))
        # Fourth GRU layer
        my_GRU_model.add(GRU(units=unit, activation='tanh'))
        # my_GRU_model.add(Dropout(0.2))
        # The output layer
        my_GRU_model.add(Dense(units=1))
        # Compiling the RNN
        my_GRU_model.compile(optimizer='adam', loss=Huber(), metrics=['acc'])

        my_GRU_model.fit(trainX, trainY, epochs=100, validation_split=0.2, batch_size=8, verbose=0)

        # my_GRU_model.save(f'./long_ts_prediction/multi_gru/{product_list[prod_index]}.h5',
                          # save_format='h5')
        my_GRU_model.save(f'./py_files/long_ts_prediction/multi_gru/{product_list[prod_index]}.h5',
                          save_format='h5')

        forecast = my_GRU_model.predict(trainX[-n_future:])
        forecast_copies = np.repeat(forecast, df_for_training.shape[1],
                                    axis=-1)  # np.repeat(a, n) a를 n개 복사, axis=-1 현재 배열의 마지막 axis
        y_pred_future = scaler.inverse_transform(forecast_copies)[:, 0]

        ## predict 반환하는 dataframe 만들어주기
        forecast_dates = []
        for time_i in forecast_period_dates:
            forecast_dates.append(time_i.date())

        len_forecast = len(forecast[:, -1])
        # forecast_dates = pd.date_range(forecast_dates[1], periods=n_future, freq = str(int(n_future/len_forecast))+'d').tolist() # <= 날짜 길이 맞지않는거 대비해  [ 예측요청일(180) / 예측값나온일 ] 해서 frequency로 데이터프레임 날짜 들어가게 띄어주기
        df_forecast = pd.DataFrame({'납기일자': pd.date_range(forecast_period_dates[1], periods=len_forecast,
                                                          freq=str(int(n_future / len_forecast)) + 'd').tolist(),
                                    '예측중량': y_pred_future})
        df_forecast['납기일자'] = pd.to_datetime(df_forecast['납기일자'])
        df_forecast['제품명'] = product_list[prod_index]

        add_to_table_final = pd.concat([add_to_table_final,df_forecast])

        # con_data = pd.concat([prod0_df, df_forecast])[['납기일자', '예측중량']]  # 전체기간~예측기간 까지의 데이터
        # con_data.reset_index(inplace=True, drop=True)
        # con_data

add_to_table_final.sort_values(by='납기일자',inplace=True)
add_to_table_final.set_index('납기일자',inplace=True)
# db update
replace_table(add_to_table_final,'중장기수주예측')
