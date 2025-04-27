import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta

# 페이지 설정
st.set_page_config(
    page_title="백화점 방문 패턴 분석 대시보드",
    page_icon="🏬",
    layout="wide"
)

# 데이터베이스 테이블명 정의
TABLE_NAME = "RESIDENTIAL__WORKPLACE_TRAFFIC_PATTERNS_FOR_SNOWFLAKE_STREAMLIT_HACKATHON"

# 앱 제목 설정
st.title("🏬 서울 주요 백화점 방문 패턴 분석 대시보드")
st.markdown("*서울 주요 백화점 방문 데이터 분석 결과와 예측 모델*")

# 세션 설정 - 스노우플레이크 환경에서는 이미 활성화된 세션이 있음
session = get_active_session()

# 데이터 로드 함수
@st.cache_data(ttl=3600)  # 1시간 캐시
def load_visit_data():
    # 백화점 방문 데이터 쿼리
    df_visits = session.sql("""
        SELECT 
            DATE_KST, 
            DEP_NAME, 
            COUNT AS visitors
        FROM DEPARTMENT_STORE_FOOT_TRAFFIC_FOR_SNOWFLAKE_STREAMLIT_HACKATHON.PUBLIC.SNOWFLAKE_STREAMLIT_HACKATHON_LOPLAT_DEPARTMENT_STORE_DATA
    """).to_pandas()
    return df_visits

@st.cache_data(ttl=3600)
def load_location_data():
    # 주거지/근무지 데이터 쿼리
    df_location = session.sql("""
        SELECT 
            DEP_NAME, 
            LOC_TYPE, 
            ADDR_LV2, 
            ADDR_LV3, 
            RATIO
        FROM RESIDENTIAL__WORKPLACE_TRAFFIC_PATTERNS_FOR_SNOWFLAKE_STREAMLIT_HACKATHON.PUBLIC.SNOWFLAKE_STREAMLIT_HACKATHON_LOPLAT_HOME_OFFICE_RATIO
    """).to_pandas()
    return df_location

# 로딩 스피너 표시
with st.spinner("데이터를 불러오는 중..."):
    try:
        # 데이터 로드
        visits_df = load_visit_data()
        visits_df.rename(columns={'VISITORS': 'visitors'}, inplace=True)
        visits_df['DATE_KST'] = pd.to_datetime(visits_df['DATE_KST'])
        visits_df['year'] = visits_df['DATE_KST'].dt.year
        visits_df['month'] = visits_df['DATE_KST'].dt.month
        visits_df['day_of_week'] = visits_df['DATE_KST'].dt.day_name()
        location_df = load_location_data()
        
        # 데이터가 성공적으로 로드됨
        st.success("데이터가 성공적으로 로드되었습니다! 아래에서 분석 결과를 확인하세요.")
    except Exception as e:
        st.error(f"데이터 로드 중 오류가 발생했습니다: {e}")
        st.stop()

# 사이드바 - 필터 설정
st.sidebar.header("필터 설정")

# 날짜 범위 선택
min_date = visits_df['DATE_KST'].dt.date.min()
max_date = visits_df['DATE_KST'].dt.date.max()

date_range = st.sidebar.date_input(
    "날짜 범위 선택",
    [min_date, max_date],
    min_value=min_date,
    max_value=max_date
)

# 날짜 필터링 수정 - 오류 수정
filtered_visits = visits_df.copy()
if len(date_range) == 2:
    start_date, end_date = date_range
    # datetime.date 객체를 pandas datetime으로 변환하여 비교
    filtered_visits = filtered_visits[
        (filtered_visits['DATE_KST'].dt.date >= start_date) & 
        (filtered_visits['DATE_KST'].dt.date <= end_date)
    ]

# 백화점 선택
store_options = visits_df['DEP_NAME'].unique().tolist()
selected_stores = st.sidebar.multiselect(
    "백화점 선택",
    options=store_options,
    default=store_options
)

if selected_stores:
    filtered_visits = filtered_visits[filtered_visits['DEP_NAME'].isin(selected_stores)]
    filtered_location = location_df[location_df['DEP_NAME'].isin(selected_stores)]
else:
    filtered_location = location_df

# 탭 생성 - 날씨 영향 탭 제거
tab1, tab2, tab3 = st.tabs(["📊 방문 트렌드", "🗺️ 지역 분석", "📈 방문 예측"])

# 탭 1: 방문 트렌드
with tab1:
    st.header("📊 백화점 방문 트렌드 분석")
    
    # 백화점별 총 방문객 수 - KPI 카드 형태로 표시
    st.subheader("백화점별 총 방문객 수")
    
    # 총 방문객 수 계산
    store_totals = filtered_visits.groupby('DEP_NAME')['visitors'].sum().reset_index()
    
    # KPI 카드 생성
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if '신세계_강남' in store_totals['DEP_NAME'].values:
            shinshegae_total = store_totals[store_totals['DEP_NAME'] == '신세계_강남']['visitors'].values[0]
            st.metric(label="신세계 강남", value=f"{shinshegae_total:,.0f}명")
    
    with col2:
        if '더현대서울' in store_totals['DEP_NAME'].values:
            hyundai_total = store_totals[store_totals['DEP_NAME'] == '더현대서울']['visitors'].values[0]
            st.metric(label="더현대서울", value=f"{hyundai_total:,.0f}명")
    
    with col3:
        if '롯데백화점_본점' in store_totals['DEP_NAME'].values:
            lotte_total = store_totals[store_totals['DEP_NAME'] == '롯데백화점_본점']['visitors'].values[0]
            st.metric(label="롯데백화점 본점", value=f"{lotte_total:,.0f}명")
    
    # 일별 방문 추이 (데이터 다운샘플링)
    st.subheader("일별 방문 추이")
    daily_visits = filtered_visits.groupby(['DATE_KST', 'DEP_NAME'])['visitors'].sum().reset_index()
    
    # 데이터 다운샘플링 (주 단위로)
    daily_visits['week'] = daily_visits['DATE_KST'].dt.isocalendar().week
    daily_visits['year'] = daily_visits['DATE_KST'].dt.isocalendar().year
    weekly_visits = daily_visits.groupby(['year', 'week', 'DEP_NAME'])['visitors'].mean().reset_index()
    
    # 주차 표시를 위한 날짜 생성 (각 주의 월요일)
    weekly_visits['date_display'] = weekly_visits.apply(
        lambda row: datetime.fromisocalendar(int(row['year']), int(row['week']), 1), axis=1
    )
    
    fig_weekly = px.line(
        weekly_visits, 
        x='date_display', 
        y='visitors', 
        color='DEP_NAME',
        title="주별 백화점 방문객 수",
        labels={'date_display': '날짜 (주 단위)', 'visitors': '평균 방문객 수', 'DEP_NAME': '백화점'},
        color_discrete_map={'신세계_강남': '#FF3A5F', '더현대서울': '#00A699', '롯데백화점_본점': '#6495ED'}
    )
    
    # x축 날짜 형식 지정
    fig_weekly.update_layout(
        xaxis=dict(
            tickformat="%Y-%m-%d",
            tickangle=45,
            tickmode="auto",
            nticks=20
        )
    )
    
    st.plotly_chart(fig_weekly, use_container_width=True)
    
    # 요일별 평균 방문객 수
    st.subheader("요일별 평균 방문객 수")
    
    # 요일 순서 정의
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_korean = {'Monday': '월요일', 'Tuesday': '화요일', 'Wednesday': '수요일', 
                 'Thursday': '목요일', 'Friday': '금요일', 'Saturday': '토요일', 'Sunday': '일요일'}
    
    weekday_avg = filtered_visits.groupby(['day_of_week', 'DEP_NAME'])['visitors'].mean().reset_index()
    weekday_avg['day_of_week'] = pd.Categorical(weekday_avg['day_of_week'], categories=day_order, ordered=True)
    weekday_avg = weekday_avg.sort_values('day_of_week')
    
    # 한글 요일 추가
    weekday_avg['day_korean'] = weekday_avg['day_of_week'].map(day_korean)
    
    fig_weekday = px.bar(
        weekday_avg, 
        x='day_korean', 
        y='visitors', 
        color='DEP_NAME',
        barmode='group',
        title="요일별 평균 방문객 수",
        labels={'day_korean': '요일', 'visitors': '평균 방문객 수', 'DEP_NAME': '백화점'},
        color_discrete_map={'신세계_강남': '#FF3A5F', '더현대서울': '#00A699', '롯데백화점_본점': '#6495ED'}
    )
    st.plotly_chart(fig_weekday, use_container_width=True)
    
    # 월별 방문 트렌드
    st.subheader("월별 방문 트렌드")
    
    # 월별 총 방문객 집계
    monthly_visits = filtered_visits.groupby(['year', 'month', 'DEP_NAME'])['visitors'].sum().reset_index()
    monthly_visits['year_month'] = monthly_visits['year'].astype(str) + '-' + monthly_visits['month'].astype(str).str.zfill(2)
    
    fig_monthly = px.line(
        monthly_visits, 
        x='year_month', 
        y='visitors', 
        color='DEP_NAME',
        title="월별 백화점 방문객 수",
        labels={'year_month': '년-월', 'visitors': '방문객 수', 'DEP_NAME': '백화점'},
        color_discrete_map={'신세계_강남': '#FF3A5F', '더현대서울': '#00A699', '롯데백화점_본점': '#6495ED'}
    )
    
    # x축 라벨 회전
    fig_monthly.update_layout(
        xaxis=dict(
            tickmode='auto',
            nticks=20,
            tickangle=45
        )
    )
    
    st.plotly_chart(fig_monthly, use_container_width=True)
    
    # 연도별 방문 추이
    st.subheader("연도별 방문 추이")
    yearly_visits = filtered_visits.groupby(['year', 'DEP_NAME'])['visitors'].sum().reset_index()
    
    fig_yearly = px.bar(
        yearly_visits, 
        x='year', 
        y='visitors', 
        color='DEP_NAME',
        barmode='group',
        title="연도별 백화점 방문객 수",
        labels={'year': '연도', 'visitors': '방문객 수', 'DEP_NAME': '백화점'},
        color_discrete_map={'신세계_강남': '#FF3A5F', '더현대서울': '#00A699', '롯데백화점_본점': '#6495ED'}
    )
    st.plotly_chart(fig_yearly, use_container_width=True)

# 탭 2: 지역 분석 (주거지/근무지)
with tab2:
    st.header("🗺️ 주거지 및 근무지 기반 방문객 분석")
    
    # 주거지/근무지 선택
    location_type = st.radio(
        "분석 유형 선택",
        options=["주거지 (Home)", "근무지 (Office)"],
        horizontal=True
    )
    
    loc_type_val = 1 if location_type == "주거지 (Home)" else 2
    
    # 필터링
    filtered_location_type = filtered_location[filtered_location['LOC_TYPE'] == loc_type_val]
    
    # 상위 지역 선택
    top_n = st.slider("상위 지역 수 선택", min_value=5, max_value=20, value=10)
    
    # 지역별 방문객 비율 시각화
    st.subheader(f"백화점별 {location_type} 방문객 상위 지역")
    
    for store in filtered_location_type['DEP_NAME'].unique():
        store_data = filtered_location_type[filtered_location_type['DEP_NAME'] == store]
        store_data = store_data.sort_values('RATIO', ascending=False).head(top_n)
        
        fig_location = px.bar(
            store_data,
            x='ADDR_LV3',
            y='RATIO',
            title=f"{store}: {location_type} 기준 방문객 상위 {top_n}개 지역",
            hover_data=['ADDR_LV2', 'ADDR_LV3', 'RATIO'],
            labels={'ADDR_LV3': '법정동', 'RATIO': '방문 비율', 'ADDR_LV2': '구'},
            color_discrete_sequence=[
                '#FF3A5F' if store == '신세계_강남' else 
                '#00A699' if store == '더현대서울' else 
                '#6495ED'
            ]
        )
        
        # x축 라벨 회전
        fig_location.update_layout(xaxis_tickangle=45)
        
        st.plotly_chart(fig_location, use_container_width=True)
    
    # 구 단위 방문객 분포
    st.subheader(f"구 단위 {location_type} 방문객 분포")
    
    # 구 단위 집계
    district_data = filtered_location_type.groupby(['DEP_NAME', 'ADDR_LV2'])['RATIO'].sum().reset_index()
    
    for store in district_data['DEP_NAME'].unique():
        store_district = district_data[district_data['DEP_NAME'] == store]
        store_district = store_district.sort_values('RATIO', ascending=False)
        
        fig_district = px.pie(
            store_district,
            values='RATIO',
            names='ADDR_LV2',
            title=f"{store}: {location_type} 기준 구 단위 방문객 분포",
            hole=0.4,
            color_discrete_sequence=px.colors.sequential.Plasma
        )
        st.plotly_chart(fig_district, use_container_width=True)

# 탭 3: 방문 예측 (원래 탭 4였음)
with tab3:
    st.header("📈 백화점 방문 예측 모델")
    
    try:
        # 간단한 시계열 예측 모델 (이동 평균 기반)
        st.subheader("백화점별 방문 예측 (30일)")
        
        # 백화점 선택
        pred_store = st.selectbox(
            "예측할 백화점 선택",
            options=store_options
        )
        
        # 선택한 백화점 데이터 필터링
        store_data = visits_df[visits_df['DEP_NAME'] == pred_store].copy()
        store_data = store_data.sort_values('DATE_KST')
        
        # DATE_KST가 datetime 타입인지 확인하고 변환
        store_data['DATE_KST'] = pd.to_datetime(store_data['DATE_KST'])
        
        # 날짜 및 방문객 수 추출
        dates = store_data['DATE_KST']
        visits = store_data['visitors']
        
        # 이동 평균 계산 (7일, 14일, 30일)
        store_data['MA7'] = visits.rolling(window=7).mean()
        store_data['MA14'] = visits.rolling(window=14).mean()
        store_data['MA30'] = visits.rolling(window=30).mean()
        
        # 전체 평균 방문객 수 계산 (대체값으로 사용)
        overall_avg = visits.mean()
        
        # 마지막 날짜 이후 30일 예측
        last_date = dates.max()
        future_dates = [last_date + timedelta(days=i) for i in range(1, 31)]
        
        # 간단한 계절성 모델 (작년 같은 기간 데이터 + 추세 반영)
        future_preds = []
        
        # 요일별 평균 방문객 수 계산 (보정 목적)
        weekday_avgs = store_data.groupby(store_data['DATE_KST'].dt.weekday)['visitors'].mean().to_dict()
        
        # 최근 30일 평균 방문객 수 (기본값으로 사용)
        recent_avg = visits[-30:].mean() if len(visits) >= 30 else overall_avg
        
        for future_date in future_dates:
            # 작년 같은 달의 데이터 찾기 (날짜 범위를 더 넓게 설정)
            last_year_date = future_date - timedelta(days=365)
            
            # 같은 달, 비슷한 일자의 데이터 찾기 (±5일)
            similar_dates = store_data[
                (store_data['DATE_KST'].dt.month == last_year_date.month) &
                (abs(store_data['DATE_KST'].dt.day - last_year_date.day) <= 5)
            ]
            
            if len(similar_dates) > 0:
                # 같은 요일 가중치 적용
                future_weekday = future_date.weekday()
                similar_dates['weekday'] = similar_dates['DATE_KST'].dt.weekday
                similar_dates['weight'] = 1.0
                similar_dates.loc[similar_dates['weekday'] == future_weekday, 'weight'] = 2.0
                
                # 가중 평균 계산
                base_pred = np.average(similar_dates['visitors'], weights=similar_dates['weight'])
                
                # 추세 반영 (연간 증가율 약 10% 가정)
                years_diff = future_date.year - last_year_date.year
                trend_factor = 1.1 ** years_diff
                
                # 계절성 상수 (크리스마스, 여름 휴가철 등 특별 기간)
                seasonal_factor = 1.0
                if future_date.month == 12 and future_date.day in range(20, 26):
                    seasonal_factor = 1.3  # 크리스마스 시즌
                elif future_date.month in [7, 8] and future_date.day in range(15, 31):
                    seasonal_factor = 1.2  # 여름 휴가철
                
                # 최종 예측값
                pred_value = base_pred * trend_factor * seasonal_factor
            else:
                # 데이터가 없으면 요일별 평균과 최근 트렌드를 고려
                weekday = future_date.weekday()
                
                # 요일별 평균이 있으면 사용, 없으면 최근 평균의 90%를 기본값으로 사용
                if weekday in weekday_avgs:
                    base_pred = weekday_avgs[weekday]
                else:
                    base_pred = recent_avg * 0.9
                
                # 계절성 상수 적용
                seasonal_factor = 1.0
                if future_date.month == 12 and future_date.day in range(20, 26):
                    seasonal_factor = 1.3
                elif future_date.month in [7, 8] and future_date.day in range(15, 31):
                    seasonal_factor = 1.2
                
                pred_value = base_pred * seasonal_factor
            
            # 최종 예측값이 최근 평균의 50% 미만이면 보정
            if pred_value < recent_avg * 0.5:
                pred_value = recent_avg * 0.8
            
            future_preds.append(pred_value)
        
        # 시각화를 위한 데이터프레임 생성
        past_data = pd.DataFrame({
            'date': dates[-90:],  # 최근 90일만 표시
            'visitors': visits[-90:],
            'type': 'actual'
        })
        
        future_data = pd.DataFrame({
            'date': future_dates,
            'visitors': future_preds,
            'type': 'predicted'
        })
        
        all_data = pd.concat([past_data, future_data])
        
        # 시계열 예측 시각화
        fig_pred = px.line(
            all_data,
            x='date',
            y='visitors',
            color='type',
            title=f"{pred_store} 방문객 예측 (30일)",
            labels={'date': '날짜', 'visitors': '방문객 수', 'type': '데이터 유형'},
            color_discrete_map={'actual': '#1F77B4', 'predicted': '#FF7F0E'}
        )
        
        # 추가 스타일링
        fig_pred.update_layout(
            xaxis=dict(
                tickformat="%Y-%m-%d",
                tickangle=45,
                tickmode="auto",
                nticks=20
            )
        )
        
        st.plotly_chart(fig_pred, use_container_width=True)
        
        # 요일별 예측 패턴
        st.subheader("요일별 예측 패턴")
        
        # 예측 데이터에 요일 추가
        future_data['day_of_week'] = [date.weekday() for date in future_data['date']]
        # 숫자를 요일 이름으로 변환
        weekday_names = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 
                         4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
        future_data['day_of_week'] = future_data['day_of_week'].map(weekday_names)
        
        # 요일별 평균 예측값
        weekday_pred = future_data.groupby('day_of_week')['visitors'].mean().reset_index()
        weekday_pred['day_of_week'] = pd.Categorical(weekday_pred['day_of_week'], categories=day_order, ordered=True)
        weekday_pred = weekday_pred.sort_values('day_of_week')
        weekday_pred['day_korean'] = weekday_pred['day_of_week'].map(day_korean)
        
        # 요일별 예측 그래프 생성
        fig_weekday_pred = px.bar(
            weekday_pred,
            x='day_korean',
            y='visitors',
            title=f"{pred_store}: 요일별 예측 방문객 수",
            labels={'day_korean': '요일', 'visitors': '예측 방문객 수'},
            color_discrete_sequence=['#FF7F0E']
        )
        
        st.plotly_chart(fig_weekday_pred, use_container_width=True)
       
        # 예측 결과 다운로드 기능
        st.subheader("예측 결과 다운로드")
       
        csv = future_data[['date', 'visitors']].to_csv(index=False)
        st.download_button(
            label="CSV로 다운로드",
            data=csv,
            file_name=f"{pred_store}_predictions.csv",
            mime="text/csv"
        )
       
        # 예측 모델 설명
        st.subheader("예측 모델 설명")
       
        st.markdown("""
        이 예측 모델은 다음 요소를 고려하여 미래 30일간의 백화점 방문객 수를 예측합니다:
       
        1. **계절성**: 작년 같은 시기의 방문 패턴을 기반으로 합니다.
        2. **요일 패턴**: 각 요일의 방문 패턴을 반영합니다 (예: 주말 방문 증가).
        3. **장기 추세**: 연간 약 10%의 성장률을 가정합니다.
        4. **특별 기간**: 크리스마스, 여름 휴가철 등 특별 기간의 방문 증가를 반영합니다.
       
        더 정확한 예측을 위해서는 마케팅 캠페인, 백화점 이벤트 등 추가 요소를 고려할 수 있습니다.
        """)
       
    except Exception as e:
        st.error(f"예측 모델 생성 중 오류가 발생했습니다: {e}")
        st.markdown("날짜 범위나 백화점 선택을 조정하고 다시 시도해보세요.")

# 결론 및 핵심 인사이트
st.markdown("---")
st.header("📌 핵심 인사이트 요약")

st.markdown("""
## 백화점별 특성

### 신세계 강남
- **핵심 고객층**: 서초구 고소득 주거지역(반포동, 잠원동) 주민
- **방문 패턴**: 주말 쇼핑 중심, 금-일요일 방문객 급증
- **경쟁 우위**: 3개 백화점 중 가장 많은 방문객

### 롯데백화점 본점
- **핵심 고객층**: 을지로, 소공동 업무지구 직장인
- **방문 패턴**: 금요일 방문 피크, 주말에는 감소
- **위치 특성**: 중구 내 업무밀집지역 위치로 인한 직장인 방문 높음

### 더현대서울
- **핵심 고객층**: 여의도 업무지구 직장인
- **방문 패턴**: 요일별 방문객 수 편차가 적음
- **위치 특성**: 여의도 금융업무지구와의 근접성이 핵심 경쟁력

## 전략적 시사점

### 마케팅 전략 방향
- **신세계 강남**: 주말 프로모션 강화, 고소득층 타겟 프리미엄 서비스
- **롯데백화점 본점**: 직장인 대상 평일/퇴근 후 프로모션, 비즈니스 고객 서비스
- **더현대서울**: 여의도 직장인 타겟 점심/퇴근 프로모션, 금융/사무직 특화 상품

### 운영 최적화 방안
- **롯데백화점 본점**: 금요일 인력 및 서비스 강화, 중구 직장인 특화 서비스
- **더현대서울**: 평일-주말 균형있는 인력 배치, 여의도 직장인 특화 서비스
""")

# 푸터
st.markdown("---")
st.caption("© 2025 백화점 방문 패턴 분석 대시보드 | 스노우플레이크 x 스트림릿 해커톤")