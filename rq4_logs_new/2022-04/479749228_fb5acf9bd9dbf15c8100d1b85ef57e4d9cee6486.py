"""
    Data Processor for Trading

    @author: Younghyun Kim
    Created on 2022.04.16
"""
import numpy as np
import pandas as pd


class OHLCVDataProcessor:
    """
        Data Processor for Trading
    """
    def __init__(self, ohlcv_data: dict = None,
                eps: float = 1e-6, translate_cols: bool = False):
        """
            Initialization

            Args:
                ohlcv_data: dict of stock candle data series
                    * dtype: dict
                    * key: asset code
                    * value: stock candle data series
                        * dtype: pd.DataFrame
                        * shape: (date_num, factor_num)
                eps: tiny number
                    * default: 1e-6
                translate_cols: translate cols kor -> eng
                    * default: False

                데이터 정규화는 dataset에서 실시
                1) price horizontal,
                2) price cross-sectional,
                3) return horizontal,
                4) return cross-sectional
        """
        self.col_translation_kr = {
            '시가': 'open',
            '고가': 'high',
            '저가': 'low',
            '종가': 'close',
            '거래량': 'volume',
            '거래대금': 'value'
        }
        self.cols = ['open', 'high', 'low', 'close', 'value']
        self.cols_num = len(self.cols)
        self.feature_num = 4 * self.cols_num

        self.eps = eps

        for key, value in ohlcv_data.items():
            if translate_cols:
                value = value.rename(
                    columns=self.col_translation_kr)
            value.index.rename('date', inplace=True)
            value = value[self.cols]

            ohlcv_data[key] = value

        self.ohlcv_data = ohlcv_data
        self.assets = list(ohlcv_data.keys())
        self.asset_num = len(self.assets)

    def calc_returns(self):
        """
            calculate returns
        """
        price = pd.concat((value['close'].rename(key)
                    for key, value in self.ohlcv_data.items()), axis=1)

        returns = price.pct_change().shift(-1).dropna()

        return returns

    def calc_feature_data(self):
        """
            calculate asset feature data

            Return:
                features: feature data of all assets
                    * dtype: np.array
                    * shape: (asset_num, date_num, feature_num)
                dates: date series
                    * dtype: np.array
                    * shape: (date_num)
        """
        features = []
        dates = None
        for key, value in self.ohlcv_data.items():
            value = np.log(value + self.eps)

            returns = value.diff(1).iloc[1:]
            value = value.iloc[1:]

            feat = pd.concat((value, value, returns, returns),
                            axis=1).values

            features.append(feat)

            if dates is None:
                dates = value.index.values

        features = np.array(
            features).reshape(self.asset_num, -1, self.feature_num)

        return features, dates

    def normalize_features(self, features):
        """
            normalize features

            Args:
                features: feature data
                    * dtype: np.array
                    * shape: (asset_num, date_num, feature_num)
        """
        num = self.cols_num
        normalized = np.zeros_like(features)

        normalized[:, :, :num] =\
            self.minmax_scaling(features[:, :, :num], axis=1)
        normalized[:, :, num:2*num] =\
            self.minmax_scaling(normalized[:, :, :num], axis=0)
        normalized[:, :, 2*num:3*num] =\
            self.minmax_scaling(features[:, :, 2*num:3*num], axis=1)
        normalized[:, :, 3*num:4*num] =\
            self.minmax_scaling(features[:, :, 3*num:4*num], axis=0)

        return normalized

    def minmax_scaling(self, features, axis=0):
        """
            minmax scaling

            Args:
                features: feature data
                    * dtype: np.array
                    * shape: (date_num, asset_num, feature_num)
                axis: target axis
                    * default: 0
        """
        nmax = features.max(axis=axis, keepdims=True)
        nmin = features.min(axis=axis, keepdims=True)

        normalized = (features - nmin) / (nmax - nmin + self.eps)

        return normalized


class StrategyProcessor:
    """
        Strategy Processor for Learning Trading

        추출 전략:
            1) 단위 시점 최대 수익률 전략
            2) 임의 전략 이후 시점 단위 시점 최대 수익률 전략
    """
    def __init__(self, returns, st_weights, fee=0.004,
                initial_t=0):
        """
            Initialization

            Args:
                returns: asset returns
                    * dtype: pd.DataFrame
                    * shape: (date_num, asset_num)
                    * 익일 수익률
                st_weights: strategy weights based on each time
                    * dtype: dict
                    * keys: strategies
                    * values: strategy weights
                        * dtype: pd.DataFrame
                        * shape: (date_num, asset_num)
                fee: trading_fee
                    * default: 0.004
                initial_t: initial time position #
                    * default: 0
                    * 관찰 데이터를 고려하여, 관찰 데이터 마지막 시점 번호를 입력하여
                    위치를 맞추기 위함
        """
        # 데이터 크기 확인
        for _, value in st_weights.items():
            assert returns.shape == value.shape

        self.returns = returns
        self.st_weights = st_weights
        self.fee = fee
        self.initial_t = initial_t

        self.strategies  = ['buy_and_hold'] + list(st_weights.keys())
        self.bah_idx = 0

        self.st_returns = self.calc_strategy_returns()

    def calc_daily_best_strategies(self, mode='max'):
        """
            calculate daily best strategies

            Args:
                mode: 수익률 목표
                    'max': max returns
                    'min': min returns
            Return:
                best_strategies: best strategy index list
                    * dtype: pd.DataFrame
                    * shape: (date_num, 1)
                pos_list: time position list
                    * dtype: np.array
                    * shape: (date_num)
        """
        best_strategies = []

        for i in range(self.returns.shape[0]):
            if mode == 'max':
                idx = self.st_returns.iloc[i].values.argmax() + 1
            elif mode == 'min':
                idx = self.st_returns.iloc[i].values.argmin() + 1

            best_strategies.append(idx)

        pos_list = np.arange(self.returns.shape[0]) + self.initial_t

        assert len(pos_list) == len(best_strategies)

        return np.array(best_strategies), pos_list

    def calc_daily_best_strategies_rebalanced(self, mode='max'):
        """
            calculate daily best strategies rebalanced

            Args:
                mode: 수익률 목표
                    'max': max returns
                    'min': min returns
            Return:
                st_series: 2-day strategies series
                    * dtype: np.array
                    * shape: (date_num - 1, strategy_num - 1, 2)
                time_pos: 2-day data time position #
                    * dtype: np.array
                    * shape: (date_num - 1, strategy_num - 1, 2)
        """
        st_series = []
        time_pos = []

        r_length = self.returns.shape[0]

        for i in range(r_length - 1):
            ret_i = self.returns.iloc[i+1].values
            for s_idx, strategy in enumerate(self.strategies[1:]):
                st_rets = []

                w_rec = self.calc_recent_weights(
                    self.st_weights[strategy].iloc[i].values,
                    self.returns.iloc[i].values)

                bah_ret = (w_rec * ret_i).sum()
                st_rets.append(bah_ret)

                for _, next_st in enumerate(self.strategies[1:]):
                    w_next = self.st_weights[next_st].iloc[i+1].values
                    wdiff = abs(w_next - w_rec).sum()

                    ret = (w_next * ret_i).sum() - (wdiff * self.fee)

                    st_rets.append(ret)

                if mode == 'max':
                    best_idx = np.argmax(st_rets)
                elif mode == 'min':
                    best_idx = np.argmin(st_rets)

                st_series.append([s_idx + 1, best_idx])
                time_pos.append([i, i + 1])

        st_series = np.array(st_series).astype(int)
        time_pos = np.array(time_pos).astype(int)

        st_series = st_series.reshape(r_length - 1, -1, 2)
        time_pos = time_pos.reshape(r_length - 1, -1, 2) + self.initial_t

        return st_series, time_pos

    def calc_strategy_returns(self):
        """
            calculate strategy returns
        """
        st_returns = pd.DataFrame(index=self.returns.index)

        for key, value in self.st_weights.items():
            rets = (self.returns * value).sum(1)

            st_returns[key] = rets

        return st_returns

    def calc_recent_weights(self, weights, rets):
        """
            calculate recent weights

            Args:
                weights: asset weights
                    * dtype: np.array
                    * shape: (asset_num)
                rets: asset returns
                    * dtype: np.array
                    *shape: (asset_num)
        """
        weights_rec = weights * (1. + rets)
        weights_rec = weights_rec / weights_rec.sum()

        return weights_rec