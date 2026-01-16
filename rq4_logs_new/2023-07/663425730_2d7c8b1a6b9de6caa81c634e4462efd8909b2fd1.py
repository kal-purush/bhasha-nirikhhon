#%%
import pandas as pd
import numpy as np
import datetime
from datetime import date
from scipy.stats import spearmanr
import joblib
import statsmodels.api as sm
from matplotlib import pyplot as plt
import seaborn as sns

from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import make_scorer
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from pytorch_tabnet.metrics import Metric
import torch

import numpy as np
import pandas as pd
import pywt

import quantstats as qs
import warnings

#%%
# 保存参数

nums_xg=30 # 训练xgboost得到的因子数量 备选：10,20
nums_fin=10 # 回归筛选的因子数量 备选：10,20
factor_quantile= 20 # 股票分组数 备选：5,10
method_return_cal = 'long_short' # 因子单日收益率估计方法 备选：'long_short','long_only','short_only'
method_weight = 'return' # 股票权重分配方法  备选：'return','equal'
method_factor = 'long' # 因子收益率计算方法 备选：'long','long_only','short_only','long_short'
backdays = 252 # 因子收益率计算周期 备选：252,126,31,7
period = 5 # 调仓周期 备选：5,7,10,20
return_period = 5 # 计算最近因子收益的周期，用于回归筛选特征 备选：5,10,20
amount_num = 5 # 股票账户数量 备选：5,7
fee_rate = 0.0003 # 手续费率 备选：0.0003,0.001
if_xgboost = False # 是否使用xgboost模型 备选：True,False


index='date' # 日期列名
y_index='return_norm' # 因变量列名
n_splits=10   # 交叉验证折数
verbose=1 # 交叉验证输出信息等级

#%%
# 使用长数据
df = pd.read_csv("F:\data\ALL_factorValues_2019-2021.csv")
df2 = pd.read_csv("F:\data\ALL_factorValues_2021-2023.csv") 
# 合并两个数据集
factordata_df = pd.concat([df, df2], axis=0)
del df, df2
factordata_df['date']=pd.to_datetime(factordata_df['date']).dt.date
factordata_df.set_index(['date', 'qscode'], inplace=True)

#%%
# 使用短数据
factordata_df = pd.read_csv("F:\data\ALL_factorValues_2021-2023.csv") 
# factordata_df = pd.read_pickle('toy_data.pkl').set_index(['date', 'qscode'])
factordata_df['date']=pd.to_datetime(factordata_df['date']).dt.date
factordata_df.set_index(['date', 'qscode'], inplace=True)

#%%
####数据预处理
# 行业所在列
index_industry = factordata_df.columns.get_loc('中证一级行业')
# 只要行业所在列，return列后面的列，并去掉vol_std_126这一列
try:
    factordata_df = factordata_df.iloc[:, index_industry:].drop(columns=['vol_std_126'])
except:
    pass
# return列所在的位置
index_return = factordata_df.columns.get_loc('return')
factordata_df = factordata_df.drop(factordata_df.columns[1:index_return], axis=1)
# 处理缺失值
factordata_df = factordata_df.groupby(level='qscode',sort=False,group_keys=False).fillna(method= 'ffill')
factordata_df = factordata_df.dropna()

#%%
################################
# 小波变换降噪
def wavelet_denoising(series, wavelet='db5', level=1, mode='symmetric'):
    # 将Series转换为numpy数组
    data = series.to_numpy()
    # 进行小波分解
    coeff = pywt.wavedec(data, wavelet, mode=mode, level=level)
    # 高频部分置为0
    coeff[-level:] = [np.zeros_like(i) for i in coeff[-level:]]
    # 重构信号
    denoised_data = pywt.waverec(coeff, wavelet, mode=mode)
    # 确保输出和输入的长度一致
    denoised_data = denoised_data[:len(data)]
    # 返回一个新的Series，保留原有的索引
    return pd.Series(denoised_data, index=series.index)

# 需要滤波的因子列
f_cols = factordata_df.columns.drop(['中证一级行业', 'return', 'log_return'])

factordata_df=factordata_df.fillna(method='ffill')

warnings.filterwarnings("ignore", category=UserWarning)

# 前504天保持原值
datelist = factordata_df.index.get_level_values('date').unique()
for date in datelist[504:]:
    print(date)
    # 选取之前所有天的数据,因为边界效应的存在，所以要多选取7天(但是用了未来信息)
    df_ = factordata_df.loc[factordata_df.index.get_level_values('date') <= date , f_cols]
    # 对每个股票的因子数据进行小波变换降噪,将date这一天的数据替换为降噪后的数据
    for col in f_cols:
        df_[col] = df_.groupby('qscode',sort=False,group_keys=False)[col].apply(lambda x: wavelet_denoising(x, wavelet='db5', level=1, mode='symmetric'))
    # 将date这一天的数据替换为降噪后的数据
    factordata_df.loc[date, col] = df_.loc[date, col]

# 重置警告过滤器
warnings.resetwarnings()

# factordata_df = factordata_df[factordata_df.index.get_level_values('date') >= datelist[486]]
# factordata_df.to_csv(r'F:\data\All_factorValues_2021_2023_filter.csv',encoding='gbk')

#%%
# 使用过滤数据
factordata_df = pd.read_csv('F:\data\All_factorValues_2021_2023_filter_7.csv',encoding='gbk')
factordata_df['date']=pd.to_datetime(factordata_df['date']).dt.date
factordata_df.set_index(['date', 'qscode'], inplace=True)
factordata_df = factordata_df.drop(columns='abn_gp') #有问题的因子

#%%
####行业中性化
# 行业哑变量
factordata_df = pd.get_dummies(factordata_df, columns=['中证一级行业'])

def industry_norm(df):
    # 准备数据
    returns = df["return"]  # 股票收益率数据
    industry_factors = df.iloc[:,df.columns.str.contains('^中证一级行业')]  # 行业因子数据
    # 拟合线性回归模型
    model = sm.OLS(returns, industry_factors)
    results = model.fit()
    r = returns - np.dot(industry_factors ,(results.pvalues <0.05)*results.params)          
    return r

# 市值中性化
def market_cap_norm(df):
    # 准备数据
    returns = df["return"]  # 股票收益率数据
    market_cap = df["lncap"]  # 市值数据
    # 因子数据
    # factor_value = df.iloc[:,~df.columns.str.contains('^中证一级行业')].drop(columns=['return','log_return','lncap']) # 因子数据
    #factors = pd.concat([market_cap, factor_value], axis=1)
    # 为因子数据添加常数项
    #factors = sm.add_constant(factors)
    # 拟合线性回归模型
    model = sm.OLS(returns, market_cap)
    results = model.fit()
    if results.pvalues[0] > 0.05:
        return returns
    residuals = returns - market_cap*results.params[0]
    return residuals

# 同时进行行业和市值中性化
def industry_market_cap_norm(df):
    # 准备数据
    returns = df["return"]  # 股票收益率数据
    industry_factors = df.iloc[:,df.columns.str.contains('^中证一级行业')]  # 行业因子数据
    market_cap = df["lncap"]  # 市值数据
    # # 因子数据
    # factor_value = df.iloc[:,~df.columns.str.contains('^中证一级行业')].drop(columns=['return','log_return','lncap']) # 因子数据
    # factors = pd.concat([industry_factors, market_cap, factor_value], axis=1)
    # # 拟合线性回归模型
    # model = sm.OLS(returns, factors)
    factors = pd.concat([industry_factors, market_cap], axis=1)
    # # 拟合线性回归模型
    model = sm.OLS(returns, factors)
    results = model.fit()
    # 方程市值的p值大于0.1，说明市值不显著，
    if results.pvalues[industry_factors.shape[-1]]> 0.1:
        return industry_norm(df)
    r = returns - np.dot(factors ,(results.pvalues <0.05)*results.params)
    return r

# 市值和行业中性化
factordata_df['return_norm'] = factordata_df.groupby(level='date',sort=False,group_keys=False).apply(market_cap_norm)
# 筛选变量
factordata_df = factordata_df.drop(columns=['log_return','lncap'])
# 删除包含行业哑变量的列,使用正则表达式，行业哑变量的列都是以中证一级行业开头的
factordata_df = factordata_df.loc[:, ~factordata_df.columns.str.contains('^中证一级行业')]

########################

# 加权一致性相关系数CCC损失函数
def ccc_loss(y_true, y_pred):
    if isinstance(y_true, np.ndarray):
        y_true = y_true.reshape(-1, 1)
        y_pred = y_pred.reshape(-1, 1)
    elif torch.is_tensor(y_true):
        y_true = y_true.detach().numpy().reshape(-1, 1)
        y_pred = y_pred.detach().numpy().reshape(-1, 1)

    y_true_mean = np.mean(y_true)
    y_pred_mean = np.mean(y_pred)
    y_true_std = np.std(y_true)
    y_pred_std = np.std(y_pred)

    cov = np.mean((y_true - y_true_mean) * (y_pred - y_pred_mean))
    rho = cov / (y_true_std * y_pred_std)

    ccc = 2 * rho * y_true_std * y_pred_std / (
        y_true_std ** 2 + y_pred_std ** 2 + (y_true_mean - y_pred_mean) ** 2
    )
    return ccc

ccc_scorer = make_scorer(ccc_loss, greater_is_better=True)
# 自定义评估指标
class CCCMetric(Metric):
    def __init__(self):
        self._name = "ccc"
        self._maximize = True

    def __call__(self, y_true, y_pred):
        y_true = y_true.reshape(-1, 1)
        y_pred = y_pred.reshape(-1, 1)
        return ccc_loss(y_true, y_pred)

ccc_metric = CCCMetric()

#####################################

# 训练XGBoost模型 来筛选特征

def rolling_train(df, index, y_index, nums_xg=20, n_splits=10, verbose=1, params=None):
    '''按日期滚动训练模型
    Args:
        df: 数据集
        nums_xg: 筛选变量的个数
        index: 日期索引 = 'date'
        n_splits: 折数
        verbose: 是否打印训练信息
        params: 模型参数（字典）
        params = {
            'max_depth': 3,
            'n_estimators': 100,
            'learning_rate': 0.1
        }
    '''
    # 按照日期索引进行排序
    df_sorted = df.sort_index(level=index)
    # 获取日期索引的唯一值
    unique_dates = df_sorted.index.get_level_values(index).unique()

    oof_preds = []  # 保存每个折叠模型在验证集上的预测结果
    oof_targets = []  # 保存每个折叠模型的真实结果
    scores = []  # 保存每个折叠模型的得分
    # 初始化特征重要性，索引为整数，添加一列为特征名，后续每一列为一折的特征重要性
    feature_importance_df = pd.DataFrame()
    feature_importance_df['feature'] = df.columns[:-1]

    # 创建TimeSeriesSplit对象，并进行划分
    tscv = TimeSeriesSplit(n_splits)

    print("开始训练模型...")

    fold_nb = 1 # 折数,从1开始计数，一直到n_splits
    for train_index, test_index in tscv.split(unique_dates):
        train_dates = unique_dates[train_index]
        test_dates = unique_dates[test_index]

        print(f"正在训练第{fold_nb}折")

        if verbose:
            print(f"训练集日期：{train_dates[0]} - {train_dates[-1]}")
        
        # 根据训练日期和测试日期选择数据
        # train_data = df_sorted.loc[train_dates] 
        train_data = df_sorted.loc[df_sorted.index.get_level_values(index).isin(train_dates)]
        # test_data = df_sorted.loc[test_dates]
        test_data = df_sorted.loc[df_sorted.index.get_level_values(index).isin(test_dates)]

        # 获取特征和目标变量
        X_train = train_data.drop(columns=[y_index])
        y_train = train_data[y_index]
        X_test = test_data.drop(columns=[y_index])
        y_test = test_data[y_index]

        # 创建XGBRegressor对象
        model = XGBRegressor()
        model.set_params(**params)

        # 用ccc_metric，训练数据和目标数据作为评估指标进行训练
        model.fit(X_train, y_train, verbose=verbose)

        # 在测试集上进行预测
        y_pred = model.predict(X_test)

        # 计算评估指标（这里使用ccc_metric作为示例）
        ccc_score = ccc_metric(y_test.values, y_pred)
        if verbose:
            print("CCC得分：", ccc_score)

        # 保存每个折叠模型在验证集上的预测结果和真实标签
        scores.append(ccc_score)
        # print(len(y_pred), len(y_test))
        oof_preds.append(y_pred)
        oof_targets.append(y_test)

        # 保存每个模型的特征重要性，保存到feature_importance_df
        feature_importances = model.feature_importances_
        feature_importance_df[f'fold_{fold_nb}'] = feature_importances

        fold_nb += 1

    # 计算所有折叠模型的平均得分
    mean_score = np.mean(scores)
    print("平均得分：", mean_score)

    # 平均相关系数
    corr = []
    for task_id in range(len(oof_targets)):
        corr.append(np.corrcoef(oof_targets[task_id],
                                oof_preds[task_id])[0, 1])
    mean_corr = np.mean(corr)
    print(f"平均相关系数：{mean_corr }")

    # 计算特征重要性的均值,按照特征来求均值（每一列的均值）,作为行添加到feature_importance_df中
    feature_importance_df['mean'] = feature_importance_df.mean(axis=1)
    feature_importance_df = feature_importance_df.sort_values(by='mean', ascending=False)

    # 可视化特征重要性
    plt.figure(figsize=(16, 12))
    sns.barplot(x='mean', y='feature', data=feature_importance_df)

    # 保存最重要的nums_xg个特征
    top_20_features = feature_importance_df['feature'][:nums_xg].values

    print("训练结束！")

    return [mean_score, mean_corr , top_20_features]

# params
params = {
    'max_depth': 3,
    'n_estimators': 100,
    'learning_rate': 0.1,
    'eval_metric': ccc_metric
}

if if_xgboost:
    # 用rolling_train函数训练模型
    res = rolling_train(factordata_df.drop(columns='return'), index=index, y_index=y_index,nums_xg=nums_xg, n_splits=n_splits, verbose=verbose, params=params )
    # 用最优模型来筛选特征
    top_20_features = res[2]
else:
    top_20_features = ['exp_wgt_momentum_1m', 'trix_15', 'bb_pos_20', 'williams_20', 'macd_histogram_12_26', 'macd_12_26', 'cci_20', 'wgt_momentum_1m', 'eom_20', 'vol_std_21', 'turnover_std_1m', 'vr_20', 'ma_20', 'bb_range_20', 'exp_wgt_momentum_3m', 'vol_std_63', 'cmo_20', 'qstick_20', 'pet_20', 'ep']

##############################
#%%

####计算因子收益率矩阵（做了修改调整）
def factor_return(df, factor_name=top_20_features, factor_quantile=5, method_return_cal = 'long_short', backdays = 252):
    factors_r = pd.DataFrame(columns=factor_name)
    for factor in factor_name:
        #df是因子值矩阵
        # 加入rank of return 避免重复值
        df['factor_quantile_'+factor] = df.groupby(level='date', sort = False, group_keys = False).\
            apply(lambda x: pd.qcut(0.9999*x[factor].rank()+0.0001*x['return_norm'].rank(), factor_quantile, labels=False, duplicates='drop'))
        df = df.dropna()
        # 1月1号构建的组合，利用1月2号的收益率来计算，保存为1月1号的因子收益率
        ##################################
        ##################################
        if method_return_cal == 'long_short': # 买顶卖底
            factors_r[factor] = df.groupby('date', sort = False, group_keys = False).apply(lambda x: \
                x[x['factor_quantile_'+factor]==factor_quantile-1]['forward_return_norm'].mean()-x[x['factor_quantile_'+factor]==0]['forward_return_norm'].mean())
        elif method_return_cal == 'long':
            factors_r[factor] = df.groupby('date', sort = False, group_keys = False).apply(lambda x: \
                x[x['factor_quantile_'+factor]==factor_quantile-1]['forward_return_norm'].mean()-x[x['factor_quantile_'+factor]==0]['forward_return_norm'].mean())
        elif method_return_cal == 'long_only': # 买顶
            factors_r[factor] = df.groupby('date', sort = False, group_keys = False).apply(lambda x: \
                x[x['factor_quantile_'+factor]==factor_quantile-1]['forward_return_norm'].mean())
        elif method_return_cal == 'short_only': # 卖底
            factors_r[factor] = df.groupby('date', sort = False, group_keys = False).apply(lambda x: \
                -1*x[x['factor_quantile_'+factor]==0]['forward_return_norm'].mean())
    fac_mom = (factors_r + 1).rolling(backdays).apply(lambda x: x.prod()) # day1用到了day2的return
    fac_mom_5 = (factors_r + 1).rolling(5).apply(lambda x: x.prod())
    fac_mom_ternary = fac_mom.copy() #先不用dropna
    fac_mom_ternary[fac_mom_ternary <= 1] = -1
    fac_mom_ternary[fac_mom_ternary > 1] = 1
    return factors_r, df, fac_mom, fac_mom_ternary

# 筛选特征，将每个因子过去一年的因子收益率与最近一段时间内的因子收益率之间回归
def feature_collect(df, factors_r_all,factor_name = top_20_features, nums_fin = 10, start_date = datetime.date(2022,6,15), return_period = 7):
    coefficients = []
    t = []
    # df 是 fac_mom, 取start_date之前的数据 ,shift 1 day,避免使用未来数据
    df = df.shift(1).loc[:start_date]
    df2 = factors_r_all.loc[:start_date]
    for factor in factor_name:##后续改为factor_name
        X = sm.add_constant(df[factor])
        y = df2[factor]
        cleaned_data = pd.concat([X, y], axis=1).dropna()
        X = cleaned_data.iloc[:,:-1]
        y = cleaned_data.iloc[:,-1]
        model = sm.OLS(y, X).fit()
        #提取回归系数
        coefficients.append(model.params[factor])
        #提取t值
        t.append(model.tvalues[factor])
    coefficients = pd.DataFrame(coefficients, index=factor_name, columns=['coefficient'])
    t = pd.DataFrame(t, index=factor_name, columns=['t'])
    top_features = coefficients.sort_values(by='coefficient', ascending= False).index[:nums_fin] #这里的10是参数

    return top_features, coefficients, t


factordata_df['forward_return_norm'] = factordata_df.groupby(level='qscode')['return_norm'].shift(-1)
factordata_df['forward_return_raw'] = factordata_df.groupby(level='qscode')['return'].shift(-1)
factordata_df = factordata_df.dropna() #做了修改调整

# 所有因子的特征名称,去掉return,return_norm和forward_return_raw列
all_features = factordata_df.columns.drop(['return','return_norm','forward_return_raw','forward_return_norm'])
factors_r_all, factordata_df, fac_mom, fac_mom_ternary = factor_return(factordata_df,factor_name=all_features, factor_quantile=factor_quantile,method_return_cal=method_return_cal, backdays=backdays)

# factors_r_all.to_csv('factors_r_all.csv')
# factordata_df.to_csv('factordata_df.csv')
# factors_r_all = pd.read_csv('factors_r_all.csv',index_col=0)
# factordata_df = pd.read_csv('factordata_df.csv',index_col=[0,1])

# top_20_features =['exp_wgt_momentum_1m',
#  'williams_20',
#  'bb_pos_20',
#  'trix_15',
#  'macd_histogram_12_26',
#  'macd_12_26',
#  'cci_20',
#  'abn_pre',
#  'wgt_momentum_1m',
#  'eom_20',
#  'vr_20',
#  'ma_20',
#  'vol_std_21',
#  'turnover_std_1m',
#  'bb_range_20',
#  'pr',
#  'cmo_20',
#  'exp_wgt_momentum_3m',
#  'vol_std_63',
#  'or',
#  'qstick_20',
#  'current',
#  'ado_20',
#  'tte_ttm',
#  'turnover_std_3m',
#  'apr_ttm',
#  'pe_ttm',
#  'quick',
#  'ncf',
#  'cap']

#%%
def feature_collect(df, factors_r_all,factor_name = top_20_features, nums_fin = 10, start_date = datetime.date(2022,6,15)):
    coefficients = []
    t = []
    # df 是 fac_mom, 取start_date之前的数据 ,shift 1 day,避免使用未来数据
    df = df.shift(1).loc[:start_date]
    df2 = factors_r_all.loc[:start_date]
    for factor in factor_name:##后续改为factor_name
        X = sm.add_constant(df[factor])
        y = df2[factor]
        cleaned_data = pd.concat([X, y], axis=1).dropna()
        X = cleaned_data.iloc[:,:-1]
        y = cleaned_data.iloc[:,-1]
        model = sm.OLS(y, X).fit()
        #提取回归系数
        coefficients.append(model.params[factor])
        #提取t值
        t.append(model.tvalues[factor])
    coefficients = pd.DataFrame(coefficients, index=factor_name, columns=['coefficient'])
    t = pd.DataFrame(t, index=factor_name, columns=['t'])
    top_features = coefficients.sort_values(by='coefficient', ascending= False).index[:nums_fin] #这里的10是参数

    return top_features, coefficients, t

if if_xgboost:
    # 用xgboost来筛选特征
    top_features, coefficients, t = feature_collect(fac_mom, factors_r_all, factor_name=top_20_features, nums_fin=nums_fin, start_date = datetime.date(2022,6,15))
    factors_r, factordata_df2, fac_mom, fac_mom_ternary = factor_return(factordata_df,factor_name=top_features, factor_quantile=factor_quantile,method_return_cal=method_return_cal, backdays=backdays)
else:
    # 用回归系数来筛选特征
    top_features, coefficients, t = feature_collect(fac_mom, factors_r_all, factor_name=all_features, nums_fin=nums_fin, start_date = datetime.date(2022,6,15))
    factors_r, factordata_df2, fac_mom, fac_mom_ternary = factor_return(factordata_df,factor_name=top_features, factor_quantile=factor_quantile,method_return_cal=method_return_cal, backdays=backdays)

# 名称包含top_features的变量
top_features_qt = ['factor_quantile_'+factor for factor in list(top_features)]
factordata_df2 = factordata_df2[[*top_features_qt ,'return_norm','return','forward_return_norm','forward_return_raw']]

# 计算每列的累积乘积
def plot_cumprod(factors_r):
    df_cumprod = (factors_r+1).cumprod()

    plt.figure(figsize=(20, 6))

    # 遍历每个列并绘制折线图
    for column in df_cumprod.columns:
        plt.plot(df_cumprod.index, df_cumprod[column], label=column)

    plt.xlabel('日期')  # 设置 x 轴标签
    plt.ylabel('累积乘积')  # 设置 y 轴标签
    plt.title('折线图')  # 设置图表标题
    plt.legend()  # 显示图例
    plt.xticks(rotation=45)  # 旋转 x 轴刻度标签，使其更易读

    plt.show()

plot_cumprod(factors_r_all)

plot_cumprod(factors_r_all[[*top_20_features]])

plot_cumprod(factors_r_all[[*top_features]])

########################## 回测

# 归一化函数
def trans(x, method_factor = 'long_short'):
    """
    method: long_short, long_only, long
    return: fac_mom_ternary
    输入：一天的因子收益率值
    输出：归一化的因子权重值
    """
    # 用以计算归一化后的因子权重
    # long_short: sum (weight<0) = -1/2, sum (weight>0) = 1/2 分方向归一化后，再一起归一化， w有正有负，不作为投资建议，因为无法实现，只能用来判断因子是否有效,正收益说明策略有效
    # long: sum abs(weight) = 1, w有正有负 做空的因子变成买底部的，做多的因子变成买顶部的
    # long_only : sum weight = 1, w全正,卖空的因子为0
    # short_only: sum weight = 1, w全正,做多的因子为0
    if method_factor == 'long_short':
        negative_mask = x < 0
        positive_mask = x > 0
        negative_sum = x[negative_mask].sum()
        positive_sum = x[positive_mask].sum()
        if negative_sum != 0:
            x[negative_mask] = -x[negative_mask] / negative_sum
        if positive_sum != 0:
            x[positive_mask] = x[positive_mask] / positive_sum
        x = x / x.abs().sum()
    elif method_factor == 'long_only':
        x[x < 0] = 0
        if x[x > 0].sum() != 0:
            x = x / x[x > 0].sum()
    elif method_factor == 'short_only':
        x[x > 0] = 0
        if x[x < 0].sum() != 0:
            x =  x / x[x < 0].sum()
    elif method_factor == 'long':
        x = x / x.abs().sum()
    else:
        print('method error')
    return x

def get_weight(fac_mom,fac_mom_ternary,method_weight = 'equal',method_factor = 'long'):  ###做了修改调整
    """
    fac_mom_ternary: 每个因子是否买入的矩阵
    method: 权重分配方法
    return: 每个因子的权重 days * factors ,shape = len(days-1)*len(factors)，有正有负
    """

    # 1月2号的权重应该基于1月1号的因子值，因为用了1月2号的收益率来计算1月1号的因子收益率
    # 每个因子买卖的权重
    if method_weight == 'equal':
        weight = fac_mom_ternary.shift(1).apply(lambda x : trans(x,method_factor=method_factor), axis=1)
    elif method_weight == 'return':
        weight = (fac_mom-1).shift(1).apply(lambda x : trans(x,method_factor=method_factor), axis=1)

    weight.columns =  ['factor_quantile_'+ i for i in weight.columns]

    return weight

# 每个股票的权重
def get_stock_weight(df, method_weight='equal'):
    """
    weight: 每个因子买卖的权重
    group_df: 每个股票的分组
    return: 每个股票的权重,大于0,sum(abs(w)) = 1 days * stocks
    """
    # 默认方法：按当日收益率来分配当日的权重
    stock_weight = df['return_norm'].unstack(level='qscode').apply(lambda x: x / x.sum(), axis=1)
    # 每个股票的权重
    # 等权重，忽略NA
    if method_weight == 'equal':
        stock_weight = stock_weight.apply(lambda x: (1-pd.isna(x))/ x.count(), axis=1)
    # 按一个月内的收益率分配权重，实质是波动性大的权重多一点
    ##########################################################
    ##########################################################
    elif method_weight == 'return':
        stock_weight = stock_weight.apply(lambda x: x.abs() / x.abs().sum(), axis=1)
    ############################## 缓和一点的方法
    stock_weight = stock_weight.fillna(0)
    return stock_weight

# 都是当日收盘时的权重
factor_weight = get_weight(fac_mom,fac_mom_ternary,method_weight = method_weight,method_factor = method_factor)  #做了修改调整
stock_weight = get_stock_weight(factordata_df2, method_weight=method_weight) ##原本是df2

###############################  合并成每个组合中股票的权重
def get_weight_stock_final(factor_df,stock_weight,factor_weight,method_factor='long',quantile=5):
    #数据预处理
    df = factor_df.copy()
    #当method == 'long_only'时，将尾部那组替换为0，否则应该替换为-1以卖空
    df[df.filter(regex='factor_quantile').columns] = df.filter(regex='factor_quantile').replace(0, -1 * (method_factor != 'long_only')) 
    df[df.filter(regex='factor_quantile').columns] = df.filter(regex='factor_quantile').replace(range(1, quantile-1), 0)
    #当method == 'short_only'时，将头部那组替换为0，否则应该替换为1以买入
    df[df.filter(regex='factor_quantile').columns] = df.filter(regex='factor_quantile').replace(quantile-1, 1 * (method_factor != 'short_only'))

    weight_s = stock_weight.stack()
    weight_s.name = 'weight'
    df = pd.concat([df.reindex(weight_s.index),weight_s],axis=1).dropna(how = 'all')

    # 每一天每只股票每个因子的权重 = 每一天每只股票的权重 点乘 每一天每个股票每个因子的买卖情况
    for i in df.filter(regex='factor_quantile').columns:
        df[i] = df['weight'] * df[i]

    # 每一天每一只股票的权重
    weight_stock_day = stock_weight.copy()

    if method_factor == 'long_short':
        for date in stock_weight.index:
            # day1 的数据
            f = factor_weight.loc[date]
            d = df.loc[df.index.get_level_values(0)==date].filter(regex='factor_quantile')
            # 保留所有weight，按因子加权求和
            dsf = d.mul(np.sign(f),axis=1).fillna(0).apply(lambda x: x/x.abs().sum(), axis=0).fillna(0)
            # 按因子加权求和
            weight_stock_day.loc[date]= np.dot(dsf,f.abs().T)
    else: # method == 'long_only' or 'short_only','long'
        for date in stock_weight.index:
            # day1 的数据
            f = factor_weight.loc[date]
            d = df.loc[df.index.get_level_values(0)==date].filter(regex='factor_quantile')
            # 乘以正负号判断是否保留，保留大于0的权重并在同一个因子内归一化
            dsf = d.mul(np.sign(f),axis=1).apply(lambda x: x*(x>0), axis=0).fillna(0).apply(lambda x: x/x.sum(), axis=0).fillna(0)
            # 按因子加权求和
            weight_stock_day.loc[date]= np.dot(dsf,f.abs().T)

    return weight_stock_day
stock_weight_tol = get_weight_stock_final(factordata_df2,stock_weight,factor_weight,method_factor=method_factor,quantile=factor_quantile)
stock_weight_tol = stock_weight_tol.dropna(how='all')
stock_weight_tol = stock_weight_tol.fillna(0)

#生成记录投资组合收益率的dataframe
#回测需要注意不同日期有数据的股票并不完全相同，在引入权重时须尤其注意

def backtest(df, stock_weight_tol, period = 5, amount_num = 5, fee_rate = 0.0003):
    col = ['Position' + str(i) for i in range(1, amount_num +1)] # 不同的股票帐号
    portfolio_r = pd.DataFrame(0, columns =col, index = stock_weight_tol.index) 

    weight_shift = pd.Series(np.zeros(len(stock_weight_tol)))
    weight_shift.index = stock_weight_tol.index
        #######################################################
    #每天按顺序更新其中一个组合，并记录每个组合每天的成分股和该组合当天的收益率（目前求的是等权加总）
    #后续引入股票权重时，直接在merge_data中前20列乘以相应的股票权重
    for i in range(len(col)):
        position_index = ([0]* i)+([j // period * period + i for j in range(0, len(stock_weight_tol))])
        position_index = position_index[:len(stock_weight_tol)]
        w = stock_weight_tol.iloc[position_index, :] # days*stocks
        w.iloc[:i, :] = 0
        # calculate turnover rate
        w_change = w.diff().abs().sum(axis=1).fillna(0)
        w_change.index = stock_weight_tol.index
        weight_shift = weight_shift.add(w_change, level = 'date')
        # 用下一天的收益率计算当天的持仓收益率，因为是收盘后才能知道当天买入的持仓
        # 实际29号收盘买入，30号的持仓收益率保存到了29号
        return_ = df.loc[(stock_weight_tol.index, slice(None)), :]['forward_return_raw'].unstack() # days*stocks
        return_ = return_.fillna(0)
        dfnew=pd.concat([w,return_],keys=['weight','return'],axis=0)
        # 串成序列
        res =pd.Series(np.diag(np.dot(dfnew.loc['weight'],dfnew.loc['return'].T)),index=stock_weight_tol.index)
        # 把res添加到portfolio_r中，按照index对齐
        portfolio_r.iloc[:,i] = portfolio_r.iloc[:,i].add(res,fill_value=0)
        # 计算换手率
        # turnover = abs(w - w.shift(1)).sum(axis = 1).sum() / 2

    portfolio_r['total_return'] = portfolio_r.mean(axis = 1)
    portfolio_r['turnover'] = weight_shift / period
    portfolio_r['total_return'] = portfolio_r['total_return'] - portfolio_r['turnover'] * fee_rate
    portfolio_r['compound_return'] = (portfolio_r.total_return+1).cumprod() - 1
    portfolio_r['benchmark'] = df.loc[(portfolio_r.index, slice(None)), :]['forward_return_raw'].groupby(level='date').mean()
    return portfolio_r

portfolio_r = backtest(factordata_df2, stock_weight_tol, period = period, amount_num = amount_num, fee_rate = fee_rate)
portfolio_r

# 绘制换手率图
fig = plt.figure(figsize=(16, 9))
ax1 = fig.add_subplot(111)
ax1.plot(portfolio_r['turnover'], color='red', label='turnover')
ax1.legend(loc='upper left')

# 绘制因子权重图

portfolio_r[portfolio_r.iloc[:,:-1].sum(axis=1)!=0]

# stock_weight_tol.to_csv('D:\杨钦\计算机语言与笔记\quant\牧鑫\stock_weight_tol.csv',encoding='gbk')
# df4.to_csv('D:\杨钦\计算机语言与笔记\quant\牧鑫\df_include_return.csv',encoding='gbk')

# 画图
portfolio_r.index = pd.to_datetime(portfolio_r.index)

# 回测的开始和结束时间
start_date = factor_weight.dropna().index[0]
end_date = factor_weight.dropna().index[-1]

# 获取系统时间，只需要日期、小时和分钟
now = datetime.datetime.now()
now = now.strftime('%m%d%H%M')

# 回测区间内的数据
bt_data = portfolio_r.loc[start_date:end_date, :]

path = r'D:\杨钦\计算机语言与笔记\quant\牧鑫\res\\'
para = 'period'+str(period)+'_groupnums'+str(factor_quantile)+'_method_'+method_factor+'_'+method_weight+'_'+str(backdays)+'backdays'

# qs.reports.html(bt_data.total_return, output= path+'Res_'+method_weight+'_'+method_factor+now+'.html', title='Backtest Result_'+para)

# 超额收益
# qs.reports.html(bt_data.total_return - bt_data.benchmark, output=path+'extra return'+method_weight+'_'+method_factor+str(now)+'.html', title='Extra return Result_'+para)

# 把total_return和benchmark用qs.reports.html画图
qs.reports.html(bt_data[['total_return','benchmark']], output=path+'Res_2_'+method_weight+'_'+method_factor+now+'.html', title='Backtest Result_'+para)

# %%