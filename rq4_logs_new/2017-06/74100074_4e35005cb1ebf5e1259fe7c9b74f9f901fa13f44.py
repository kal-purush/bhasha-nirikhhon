
# -*- coding: utf-8 -*-
# @Time   : 2017/5/30
# @Author : XL ZHONG
# @File   : ClassTreeBinTools.py
from sklearn.tree import DecisionTreeClassifier, _tree
import numpy as np
from sklearn.base import BaseEstimator,TransformerMixin

class TreeBinTools(BaseEstimator,TransformerMixin):
    '''
    基于树的工具：决策树分 BIN （最优分 BIN 法）
    '''
    def __init__(self,X,y):
        self
        self.X=X
        self.y=y

    def ft_function(self,x):
        '''
        
        :param x: 这里输入的 x 是单个特征列
        :return: 返回处理替换后的特征咧
        '''
        clf = DecisionTreeClassifier(criterion="entropy", max_depth=10, max_leaf_nodes=10)
        x=x.reshape(len(x),1)
        clf.fit(x, self.y)
        count_leaf = 0
        for i in clf.tree_.children_left:
            if i == _tree.TREE_LEAF:
                count_leaf += 1
        count_leaf

        threshold = clf.tree_.threshold  # 所有的节点全部是<=
        # threshold=np.sort(threshold)[count_leaf:]
        # #这种方式不太好，如果有小于-2的排序就会出问题。
        #  -2的数目和叶子数相同 后面需要排除掉-2的值
        count = 0
        for i in threshold:
            if i == -2: count += 1

        new_threshold = list(filter(lambda x: x != -2, threshold))

        if count > count_leaf: new_threshold += [-2]

        new_threshold_2 = np.sort(new_threshold)

        print("特征的区间值：",new_threshold_2)
        # 这里是对已经有的区间插入对应的训练数据。
        thres_index = np.asarray(new_threshold_2).searchsorted(x, side='right')  # 注意这里的 right 表示的是<=
        thres_index = thres_index.ravel()
        x_new = []
        for i in range(len(x)):
            if thres_index[i] + 1 <= len(new_threshold_2):
                x_new.append(new_threshold_2[thres_index[i]])
            else:
                x_new.append(x.max())  # 这里后面可以修改
        return x_new  # 返回的数值是右侧的等于的值


    def fit_transform(self, X, y=None, **fit_params):

        X_new=np.apply_along_axis(self.ft_function,0,X)
        return X_new



if __name__ == '__main__':
    from sklearn.datasets import load_iris

    data = load_iris()
    data.keys()
    X=data["data"][:100]
    y=data["target"][:100]
    tree=TreeBinTools(X,y)
    X_new=tree.fit_transform(X,y)
    print(X_new)

















