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



from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String,DATE,FLOAT,VARCHAR,BIGINT,SMALLINT
meta = MetaData()


try:

   날씨= Table(
      '날씨', meta,
      Column('일자', DATE, primary_key = True),
      Column('기온', FLOAT),
      Column('강수량', FLOAT),
      Column('습도', FLOAT),
      Column('신적설량', FLOAT),
      Column('지역', VARCHAR(50), primary_key = True)
   )

   수주분석테이블= Table(
      '수주분석테이블', meta,
      Column('거래처코드', VARCHAR(50), primary_key = True),
      Column('제품코드', VARCHAR(50),primary_key = True),
      Column('납기일자', DATE,primary_key = True),
      Column('판매수량', FLOAT),
      Column('제품명', VARCHAR(50)),
      Column('지역', VARCHAR(50), )
   )

   중장기수주예측= Table(
      '중장기수주예측', meta,
      Column('납기일자', DATE, primary_key = True),
      Column('예측중량', FLOAT),
      Column('제품명', VARCHAR(20),primary_key = True)
   )

   중장기시각화= Table(
      '중장기시각화', meta,
      Column('납기일자', DATE, primary_key = True),
      Column('판매수량', FLOAT),
      Column('제품명', VARCHAR(50),primary_key = True),
   Column('is_pred', SMALLINT))


   중장기시각화2= Table(
      '중장기시각화2', meta,
      Column('납기일자', DATE, primary_key = True),
      Column('판매수량', FLOAT),
      Column('제품명', VARCHAR(50),primary_key = True),
   Column('is_pred', SMALLINT))



   생판계획예측= Table(
      '생판계획예측', meta,
      Column('납기일자', DATE, primary_key = True),
      Column('제품명', VARCHAR(50),primary_key = True),
      Column('예측중량', FLOAT))
   meta.create_all(engine)


   생판계획예측원재료량= Table(
      '생판계획예측원재료량', meta,
      Column('생산일자', DATE, primary_key = True),
      Column('원자재명', VARCHAR(50),primary_key = True),
      Column('예측필요중량', FLOAT))
   meta.create_all(engine)


   생판계획시각화= Table(
      '생판계획시각화', meta,
      Column('납기일자', DATE),
      Column('판매수량', FLOAT),
   Column('제품명', VARCHAR(50)),
      Column('is_pred', SMALLINT))


   #


   ##### text로 문제 없으면.. 있으면 VARCHAR로 변경후 적용
   # 제품별기준투입량= Table(
   #    '제품별기준투입량', meta,
   # Column('제품명', VARCHAR(50), primary_key = True),
   # Column('Urea-20%',FLOAT),Column('WRE-580FX(55%)',FLOAT),Column('WRE-770',FLOAT),Column('사용수',FLOAT),Column('액상리그닌',FLOAT),Column('RA-300S',FLOAT),Column('공기연행제',FLOAT),
   # Column('ACTICIDE-MVK',FLOAT),Column('M30',FLOAT),Column('PCA3000',FLOAT),Column('PC소포제',FLOAT),Column('PEMA-500FR',FLOAT),Column('PEMA-630XR',FLOAT),Column('PEMA-SRE-200(5',FLOAT),
   # Column('PEMA-WRE-600(5',FLOAT),Column('SRE-200SD-T',FLOAT),Column('WRE-550T',FLOAT),Column('글루콘산소다',FLOAT),Column('소포제(MR-130)',FLOAT),Column('PEMA-350B',FLOAT),Column('SRE-110(50%)',FLOAT),
   # Column('PEMA-550XD',FLOAT),Column('DEA',FLOAT),Column('BDG',FLOAT),Column('MONODN1501',FLOAT),Column('MONODN1502',FLOAT),Column('설탕',FLOAT))


   안전재고량기준= Table(
      '안전재고량기준', meta,
   Column('원자재명', VARCHAR(50), primary_key = True),
   Column('안전재고량',FLOAT))


   원자재재고량= Table(
      '원자재재고량', meta,
      Column('원자재명', VARCHAR(50), primary_key = True),
      Column('안전재고량',FLOAT),
      Column('원자재재고량', FLOAT),
      Column('안전재고량상태', FLOAT))



   원자재자동발주내역= Table(
      '원자재자동발주내역', meta,
      Column('원자재명', VARCHAR(50)),
      Column('원자재주문량',FLOAT),
      Column('발주일자', DATE)
             )

   건축착공면적= Table(
      '건축착공면적', meta,
      Column('TIME', VARCHAR(50), primary_key = True),
      Column('건축착공면적', FLOAT))

   meta.create_all(engine)
   print("Maria_DB에 APS 시스템의 필요 테이블들이 생성되었습니다.")
except:
   print("이미 테이블이 추가되어있습니다.")





