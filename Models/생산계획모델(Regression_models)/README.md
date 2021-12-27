# 단기 생산계획모델  
  
### 회귀모델 비교  
  
- Ridge
- Lasso
- DecisionTreeRegeressor
- RandomForestRegressor(최종모델선정)
- XGBRegressor
- LGBMRegressor

### 데이터셋  
  
중장기 수요예측모델에서 예측한 예측중량(일별 예측수요량)과  
시멘트 수요와 상관도가 높았고 계절성까지 고려된 건축착공면적을 독립변수로 활용하여  
회귀분석을 통해 조금 더 정교한 일별 수요예측을 진행  
  
### 최종 모델 선정
  
R-squared score가 가장 높았던 RandomForestRegressor을 최종 모델로 선정  
기존 시스템 대비 정확도 약 25% 개선
  
  
![스마트팩토리_발표 pptx](https://user-images.githubusercontent.com/86215668/146765618-901176bb-0d3f-4dba-901e-f68517a06ca4.jpg)
  
![스마트팩토리_발표 pptx (1)](https://user-images.githubusercontent.com/86215668/146765634-ca47e2be-fae8-486c-a6cc-ea453e585cc7.jpg)
