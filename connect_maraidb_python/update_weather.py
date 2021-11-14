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

# 데이터베이스에 있는 날씨 테이블 가져오고, 새로 추가할 시작날짜   무조건 !!!! 시작일자는 월요일, 끝일은 상관없음. 다만 새로 업데이트시, 마지막일자는 제거후 다시 시작일자를 그 마지막 일자의 다음날로 해야됨 (월요일)
import datetime as dt
from dateutil.relativedelta import relativedelta
from datetime import timedelta

#### 사용자함수  ###
def get_today():
  today = dt.datetime.today()
  today_year = today.year
  today_month = today.month
  today_day = today.day
  today_day
  today = dt.datetime(today_year,today_month,today_day)
  return today
def add_to_table(rows_to_add,table_name):
    rows_to_add.to_sql(table_name, con=engine, if_exists='append')



now = get_today()



yesterday= now - timedelta(days=1)



def date_to_str(date):
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

    return year_str + month_str + day_str



try:
    날씨기존df = pd.read_sql_table('날씨', con=engine)
    last_day = max(날씨기존df['일자'])

    if last_day == yesterday:
        check_table=-1  # process 종료

    else:
        #테이블에서 마지막 날짜를 찾고, 그 날짜 이후부터 어제까지의 날짜데이터 가져오기
        get_begin = last_day + timedelta(days=1)
        get_begin_str = date_to_str(get_begin)
        get_until_str = date_to_str(yesterday)
        print(f"{get_begin_str} ~ {get_until_str} 까지 데이터를 가져옵니다")
        check_table=1

except:     # 테이블이 비어있음으로, 어제부터 1년전까지의 날씨 데이터 불러오기
     #현재부터 3년치 가져오기    api 는 999행씩밖에 못가져오니, 2년먼저, 1년 따로 api 신청해야됨.
    get_begin_str1 = date_to_str(yesterday - relativedelta(years=3))
    get_until_str1 = date_to_str(yesterday - relativedelta(years=1))

    get_begin_str2 = date_to_str(yesterday - relativedelta(years=1) + timedelta(days=1))
    get_until_str2 = date_to_str(yesterday)

    print(f"날씨_테이블 비어있음. {get_begin_str1} ~ {get_until_str2} 까지 데이터를 가져옵니다")
    check_table = 0

if check_table != -1:
    if check_table ==0:
        for i in range(2):
            if i==0:
                get_begin_str = get_begin_str1
                get_until_str=get_until_str1
            else:
                get_begin_str = get_begin_str2
                get_until_str = get_until_str2
            ### 유훈함수 breaking
            import json
            import pandas as pd
            import requests
            import numpy as np
            # 내가 한번 쭉 확인 필요
            # '서울','경기도북부','경기도남부','인천','부산광역시','대구광역시','울산광역시','경상북도','경상남도','전라북도','전라남도',,'세종시','충청북도','충청남도','강원도'
            for index, location in enumerate(['대전광역시']):
                print(f"{location} 날씨 가져오기")
                def cat_location(x):
                    if x == '서울':
                        return 108
                    elif x == '경기도북부':
                        return 98
                    elif x == '경기도남부':
                        return 119
                    elif x == '인천':
                        return 112
                    elif x == '부산광역시':
                        return 159
                    elif x == '대구광역시':
                        return 152
                    elif x == '울산광역시':
                        return 143
                    elif x == '경상북도':
                        return 278
                    elif x == '경상남도':
                        return 263
                    elif x == '전라북도':
                        return 146
                    elif x == '전라남도':
                        return 156
                    elif x == '대전광역시':
                        return 133
                    elif x == '세종시':
                        return 239
                    elif x == '충청북도':
                        return 131
                    elif x == '충청남도':
                        return 129
                    elif x == '강원도':
                        return 114
                    else:
                        return 119  ## 해당 안될시 생판지역인 평택 기준 날씨로 변환


                location_code = cat_location(location)

                url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'

                params = {'serviceKey': 'ZKOx0KH7l+PcSZZNRvuI54pjFf5gbYeIa1ccvoUcbzlwPA7ZRd9AqYB+V6++N/urN+9OncLmDH9MvqvMu5SKbg==',
                          'pageNo': '1',
                          'numOfRows': '999',
                          'dataType': 'JSON',
                          'dataCd': 'ASOS',
                          'dateCd': 'DAY',
                          'startDt': get_begin_str,
                          'endDt': get_until_str,
                          'stnIds': location_code}

                response = requests.get(url, params=params)
                r_dict = json.loads(response.text)
                r_response = r_dict.get("response")
                r_body = r_response.get("body")
                r_items = r_body.get("items")
                r_item = r_items.get("item")

                # city = [] #도시
                time = []  # 일자
                tem = []  # 온도
                rain = []  # 강수여부
                hum = []  # 습도
                snow = []  # 합계3시간 신적설 ( 3시간동안 내린 양 )

                empty_data = pd.DataFrame()

                for i in range(len(r_item)):
                    time.append(r_item[i]['tm'])
                    # city.append(r_item[i]['stnNm'])
                    tem.append(r_item[i]['avgTa'])
                    rain.append(r_item[i]['sumRn'])
                    hum.append(r_item[i]['avgRhm'])
                    snow.append(r_item[i]['sumDpthFhsc'])

                time = pd.Series(time)
                # city = pd.Series(city)
                tem = pd.Series(tem)
                rain = pd.Series(rain)
                hum = pd.Series(hum)
                snow = pd.Series(snow)

                data = [time, tem, rain, hum, snow]

                df = pd.concat(data, axis=1)

                df.columns = ['일자', '기온', '강수량', '습도', '신적설량']
                df.replace('', 0, inplace=True)

                df['일자'] = pd.to_datetime(df['일자'])
                df['기온'] = round(df['기온'].astype('float32'), 1)
                df['강수량'] = round(df['강수량'].astype('float32'), 1)
                df['습도'] = round(df['습도'].astype('float32'), 1)
                df['신적설량'] = round(df['신적설량'].astype('float32'), 1)

                # add_df = df.set_index('일자').resample('w').mean()
                add_df = df.set_index('일자')
                add_df['지역'] = location

                if index==0:
                    add_total_df = add_df
                    print(location, f'{get_begin_str} ~ {get_until_str} 추가완료')
                else:
                    print(location, f'{get_begin_str} ~ {get_until_str} 추가완료')
                    add_total_df =pd.concat([add_total_df,add_df],axis=0)
                add_to_table(add_total_df, '날씨')

    else:
        ### 유훈함수 breaking
        import json
        import pandas as pd
        import requests
        import numpy as np

        # 내가 한번 쭉 확인 필요
        # '서울','경기도북부','경기도남부','인천','부산광역시','대구광역시','울산광역시','경상북도','경상남도','전라북도','전라남도',,'세종시','충청북도','충청남도','강원도'
        for index, location in enumerate(['대전광역시']):
            print(f"{location} 날씨 가져오기")


            def cat_location(x):
                if x == '서울':
                    return 108
                elif x == '경기도북부':
                    return 98
                elif x == '경기도남부':
                    return 119
                elif x == '인천':
                    return 112
                elif x == '부산광역시':
                    return 159
                elif x == '대구광역시':
                    return 152
                elif x == '울산광역시':
                    return 143
                elif x == '경상북도':
                    return 278
                elif x == '경상남도':
                    return 263
                elif x == '전라북도':
                    return 146
                elif x == '전라남도':
                    return 156
                elif x == '대전광역시':
                    return 133
                elif x == '세종시':
                    return 239
                elif x == '충청북도':
                    return 131
                elif x == '충청남도':
                    return 129
                elif x == '강원도':
                    return 114
                else:
                    return 119  ## 해당 안될시 생판지역인 평택 기준 날씨로 변환


            location_code = cat_location(location)

            url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'

            params = {
                'serviceKey': 'ZKOx0KH7l+PcSZZNRvuI54pjFf5gbYeIa1ccvoUcbzlwPA7ZRd9AqYB+V6++N/urN+9OncLmDH9MvqvMu5SKbg==',
                'pageNo': '1',
                'numOfRows': '999',
                'dataType': 'JSON',
                'dataCd': 'ASOS',
                'dateCd': 'DAY',
                'startDt': get_begin_str,
                'endDt': get_until_str,
                'stnIds': location_code}

            response = requests.get(url, params=params)
            r_dict = json.loads(response.text)
            r_response = r_dict.get("response")
            r_body = r_response.get("body")
            r_items = r_body.get("items")
            r_item = r_items.get("item")

            # city = [] #도시
            time = []  # 일자
            tem = []  # 온도
            rain = []  # 강수여부
            hum = []  # 습도
            snow = []  # 합계3시간 신적설 ( 3시간동안 내린 양 )

            empty_data = pd.DataFrame()

            for i in range(len(r_item)):
                time.append(r_item[i]['tm'])
                # city.append(r_item[i]['stnNm'])
                tem.append(r_item[i]['avgTa'])
                rain.append(r_item[i]['sumRn'])
                hum.append(r_item[i]['avgRhm'])
                snow.append(r_item[i]['sumDpthFhsc'])

            time = pd.Series(time)
            # city = pd.Series(city)
            tem = pd.Series(tem)
            rain = pd.Series(rain)
            hum = pd.Series(hum)
            snow = pd.Series(snow)

            data = [time, tem, rain, hum, snow]

            df = pd.concat(data, axis=1)

            df.columns = ['일자', '기온', '강수량', '습도', '신적설량']
            df.replace('', 0, inplace=True)

            df['일자'] = pd.to_datetime(df['일자'])
            df['기온'] = round(df['기온'].astype('float32'), 1)
            df['강수량'] = round(df['강수량'].astype('float32'), 1)
            df['습도'] = round(df['습도'].astype('float32'), 1)
            df['신적설량'] = round(df['신적설량'].astype('float32'), 1)

            # add_df = df.set_index('일자').resample('w').mean()
            add_df = df.set_index('일자')
            add_df['지역'] = location

            if index == 0:
                add_total_df = add_df
                print(location, f'{get_begin_str} ~ {get_until_str} 추가완료')
            else:
                print(location, f'{get_begin_str} ~ {get_until_str} 추가완료')
                add_total_df = pd.concat([add_total_df, add_df], axis=0)

        add_to_table(add_total_df,'날씨')
    print("Maria_DB의 <날씨> 테이블이 업데이트 되었습니다.")
else:
    print("이미 최신날씨가 업데이트 되어있습니다.")

