import pymysql
import pandas as pd
import streamlit as st
 
def insert_data():
    #DB 연결
    conn = pymysql.connect(
        host = 'localhost',
        user = 'squirrel',
        password = 'squirrel',
        database = 'cardb'
    )

    cur = conn.cursor()

    file_name = 'C:/Users/ljh10/Downloads/자동차등록현황보고_자동차등록대수현황 시도별 (201101 ~ 202412).xlsx'
    df = pd.read_excel(file_name, header=[0, 1],skiprows=4)

    df = df.loc[:, ~df.columns.get_level_values(1).str.contains('계')]
    df = df.loc[:, ~df.columns.get_level_values(0).str.contains('총계')]
    df = df.loc[df['시군구'].values != '계']

    # DB에서 이미 존재하는 월 데이터 가져오기
    cur.execute('SELECT month_year FROM car_monthly')
    existing_months = {row[0] for row in cur.fetchall()}  # 이미 존재하는 월 데이터를 집합으로 저장

    # 중복 제거된 데이터만 처리
    filtered_df = df[~df.iloc[:, 0].isin(existing_months)]  # 첫 번째 열(년월)이 DB에 없는 경우만 필터링

    # 필터링 결과 출력 (디버깅용)
    print(f"필터링된 데이터 개수: {len(filtered_df)}")
    print(filtered_df)

    # 엑셀 데이터 처리
    for index, row in filtered_df.iterrows():
        # 월별 테이블에 해당 월 데이터가 있는지 확인
        cur.execute(
            'select monthly_id from car_monthly where month_year = %s',
            (row[0],)
        )
        monthly_result = cur.fetchone()
        if monthly_result is None:
            cur.execute(
                'insert into car_monthly (month_year) values (%s)',
                (row[0],)   
            )
            print('car_monthly insert 실행')
            #conn.commit()
            cur.execute('select last_insert_id()')
            # 아래서 카 데이터에 들어갈 예정
            monthly_id = cur.fetchone()[0]
        else:
            monthly_id = monthly_result[0]
            
        # car_region 중복 확인
        cur.execute(
            'select region_id from car_region where sido_name = %s and sigungu_name = %s',
            (row[1], row[2])
        )
        region_result = cur.fetchone()
        if region_result is None:
            cur.execute(
                'insert into car_region (sido_name, sigungu_name) values (%s, %s)',
                (row[1], row[2])
            )

            cur.execute('select last_insert_id()')
            region_id = cur.fetchone()[0]
        else:
            region_id = region_result[0]
                
        # 차량 수 데이터 처리
        car_data_start_idx = 3  # 차량 수가 시작되는 열의 인덱스
        car_counts = row[car_data_start_idx:].values  # 차량 수 데이터

        # 차량 유형 및 사용 유형 정의
        car_types = ['승용', '승합', '화물', '특수']  # 차량 유형 리스트
        race_types = ['관용', '자가용', '영업용']  # 사용 유형 리스트
        
        for i, count in enumerate(car_counts):
            # 천 단위 구분자 ',' 제거 및 NaN 처리
            if pd.isna(count):
                continue
            count = int(str(count).replace(",", ""))  # 문자열에서 ',' 제거 후 정수로 변환

            # 차량 유형 및 사용 유형 매핑
            car_type_idx = i // len(race_types)  # 차량 유형 인덱스 계산
            race_type_idx = i % len(race_types)  # 사용 유형 인덱스 계산

            car_type = car_types[car_type_idx]
            race_type = race_types[race_type_idx]

            # 차량 유형 ID 가져오기
            cur.execute(
                'select type_id from car_type where type_name = %s',
                (car_type,)
            )
            type_result = cur.fetchone()
            if type_result is None:
                print(f"Warning: '{car_type}' not found in car_type table")
                type_id = None  # 기본값 설정
            else:
                type_id = type_result[0]

            # 사용 유형 ID 가져오기
            cur.execute(
                'select race_id from race_type where race_name = %s',
                (race_type,)
            )
            race_result = cur.fetchone()
            if race_result is None:
                print(f"Warning: '{race_type}' not found in car_race table")
                race_id = None  # 기본값 설정
            else:
                race_id = race_result[0]

            # type_id 또는 race_id가 None인 경우 삽입하지 않도록 처리
            if type_id is not None and race_id is not None:
                # 데이터 삽입
                cur.execute(
                    '''
                    insert into car_data (region_id, monthly_id, type_id, race_id, vehicle_count)
                    values (%s, %s, %s, %s, %s)
                    ''',
                    (region_id, monthly_id, type_id, race_id, count)
                )
            else:
                print(f"Skipping insertion for {car_type} {race_type} due to missing ID")

    conn.commit()
    # ---------------------------------------------- DB에 excel파일의 데이터 삽입
    #----------------------------------------------- DB에서 데이터 조회





    cur.close()
    conn.close()