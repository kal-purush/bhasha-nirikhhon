import pandas as pd
from io import StringIO
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler

class PreProcX:
    def __init__(self): 
        self.csv_data = '/logs/ref001.csv'

    def preGo(self):        

        # eżeli korzystasz ze środowiska Python 2.7, musisz
        # przekonwertować ciąg znaków do standardu unicode:
        # csv_data = unicode(csv_data)
        df = pd.read_csv(StringIO(self.csv_data))
        mms = MinMaxScaler()
        X_train_norm = mms.fit_transform(X_train)
        X_test_norm = mms.transform(X_test)

        stdsc = StandardScaler()
        X_train_std = stdsc.fit_transform(X_train)
        X_test_std = stdsc.transform(X_test)