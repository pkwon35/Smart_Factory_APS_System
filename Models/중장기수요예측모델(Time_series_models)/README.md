# 중장기 수요예측모델  
  
### 시계열모델 비교  
  
- SARIMAX
- GRU(최종모델선정)
- BILSTM  
  
### 데이터셋  
  
중장기 수요예측모델을 위한 분석용 데이터셋은  
데이터 마트에 있는 각 수주건에 대한 날짜, 판매량, 제품명 데이터를 제품별 일별 판매량 합계로 변경하여 사용  
** GRU model은 날씨 데이터의 기온데이터와 습도데이터를 함께 고려하여 분석  
  
### 최종 모델 선정  
  
RMSE와 R-squared score 기준으로 성능이 좋았던 케라스의 GRU 모델을 최종모델로 선정.
  
  
![스마트팩토리_발표 pptx (2)](https://user-images.githubusercontent.com/86215668/146763791-ebcb025a-14b4-4d1e-959f-ac96597d9943.jpg)
  
![스마트팩토리_발표 pptx (3)](https://user-images.githubusercontent.com/86215668/146763807-4b50c4ae-edc1-45d1-8017-0a1f2363ebe4.jpg)
