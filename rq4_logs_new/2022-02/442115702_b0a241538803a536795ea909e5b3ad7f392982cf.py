"""
    XGBoost実装クラス
"""
import numpy as np

import xgboost as xgb
from sklearn.metrics import mean_squared_error

class Xgboost:
    
    def create_dmatrix(self, tr_x, tr_y, va_x, va_y, test_x):
        """ 特徴量と目的変数をxgboostのデータ構造に変換する"""
        self.dtrain = xgb.DMatrix(tr_x, label=tr_y)
        self.dvalid = xgb.DMatrix(va_x, label=va_y)
        self.dtest = xgb.DMatrix(test_x)
    
    def modeling(self, num_round, params):
        """ モデル実行"""
        watchlist = [(self.dtrain, 'train'), (self.dvalid, 'eval')]
        self.model = xgb.train(params, 
                               self.dtrain, 
                               num_round, 
                               evals=watchlist)
        
    def predict(self, _type):
        """ 予測"""
        if _type == 'va':
            pred = self.model.predict(self.dvalid)
        elif _type == 'test':
            pred = self.model.predict(self.dtest)
        return pred
        
    def score(self, answer, pred):
        """ 　予測結果の出力"""
        rmse = np.sqrt(mean_squared_error(answer, pred))
        print(f'RMSE: {rmse:.4f}')