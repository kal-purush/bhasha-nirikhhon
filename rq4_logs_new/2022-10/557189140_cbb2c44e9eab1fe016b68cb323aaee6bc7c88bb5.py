from ddareung.models import DdareungModel
from ddareung.views import DdareungController
from util.common import Common


if __name__ == '__main__':
    api = DdareungController()
    while True:
        menu = Common.menu(["종료","시각화","모델링","머신러닝","베포"]) #모델링 : 전처리 + 모델 + 후처리 여러번 함
        if menu == "0": break
        elif menu == "1":
            print(" ### Visualization ###")
            model = DdareungModel()
            a = model.new_model("train.csv")
            print(f'Train type : {type(a)}')
            print(f'Train columns : {a.columns}')
            print(f'Train head : {a.head()}')
            print(f'Train null 수 : {a.isnull().sum()}')
            #Index(['id', 'hour', 'hour_bef_temperature', 'hour_bef_precipitation',
            #'hour_bef_windspeed', 'hour_bef_humidity', 'hour_bef_visibility',
            #'hour_bef_ozone', 'hour_bef_pm10', 'hour_bef_pm2.5', 'count'],
            #dtype='object')
            #hour_bef_temperature        2
            #hour_bef_precipitation      2
            #hour_bef_windspeed          9
            #hour_bef_humidity           2
            #hour_bef_visibility         2
            #hour_bef_ozone             76
            #hour_bef_pm10              90
            #hour_bef_pm2.5            117



        elif menu == "2":
            print(" ### Modeling ###")

        elif menu == "3":
            print(" ### Machine Learnig ###")

        elif menu == "4":
            print(" ### release ###")

        else:
            print(" 해당 메뉴 없음 ")