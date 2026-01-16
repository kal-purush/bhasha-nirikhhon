import numpy as np
import pandas as pd
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split, KFold
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression
from deap import creator, base, tools, algorithms
from sklearn.metrics import mean_squared_error, r2_score
from pyswarm import pso
import time

# 创建虚拟数据集
data = pd.read_csv('industrial_dataset.csv')
# 从 'data' 中分离特征和标签
data = data.iloc[:,1:]
all_feature = data.iloc[:, :-1]
all_label = data.iloc[:, -1]

X_scaled = all_feature.values
y = all_label.values

# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

# 遗传算法 + PSO 结合优化特征选择
creator.create("FitnessMax", base.Fitness, weights=(1.0,))
creator.create("Individual", list, fitness=creator.FitnessMax)

toolbox = base.Toolbox()

# 定义遗传算法的操作：交叉、变异、评价等
def evaluate(individual):
    model = LinearRegression()
    model.fit(X_train[:, np.where(individual)[0]], y_train)
    y_pred = model.predict(X_test[:, np.where(individual)[0]])
    mse = -mean_squared_error(y_test, y_pred)
    return mse,

# def evaluate(individual):
#     model = LinearRegression()
#     model.fit(X_train[:, np.where(individual)[0]], y_train)
#     y_pred = model.predict(X_test[:, np.where(individual)[0]])
#     r2 = r2_score(y_test, y_pred)
#     return r2,

toolbox.register("attr_bool", np.random.randint, 0, 2)
toolbox.register("individual", tools.initRepeat, creator.Individual, toolbox.attr_bool, n=50)
toolbox.register("population", tools.initRepeat, list, toolbox.individual)
toolbox.register("evaluate", evaluate)
toolbox.register("mate", tools.cxTwoPoint)
toolbox.register("mutate", tools.mutFlipBit, indpb=0.05)
toolbox.register("select", tools.selTournament, tournsize=3)

def pso_fitness(selected_features):
    score, = evaluate(selected_features)
    return -score,  # 负号因为 DEAP 的遗传算法是最小化问题

def ga_pso(num_features):
    def objective_function(selected_features):
        return pso_fitness(selected_features)[0],

    lb = [0] * num_features
    ub = [1] * num_features

    best_features, _ = pso(objective_function, lb, ub, maxiter=50, swarmsize=30)
    return best_features

def train_and_evaluate(X_train, X_test, y_train, y_test):
    model = LinearRegression()
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r_squared = r2_score(y_test, y_pred)
    return mse, r_squared

def main():
    pop = toolbox.population(n=50)
    hof = tools.HallOfFame(1)
    stats = tools.Statistics(lambda ind: ind.fitness.values)
    stats.register("avg", np.mean)
    stats.register("min", np.min)

    algorithms.eaSimple(pop, toolbox, cxpb=0.5, mutpb=0.2, ngen=80, stats=stats, halloffame=hof)

    best_individual = hof[0]
    selected_indices = np.where(best_individual)[0]
    print("选择的特征索引：", selected_indices)
    print("选择的特征个数：", len(selected_indices))
    k_fold = KFold(n_splits=8, shuffle=True, random_state=42)
    mse_scores = []
    r_squared_scores = []
    start_time = time.time()
    for train_index, test_index in k_fold.split(X_scaled):
        X_train, X_test = X_scaled[train_index], X_scaled[test_index]
        y_train, y_test = y[train_index], y[test_index]

        # 使用选定的特征进行训练和评估
        X_train_selected = X_train[:, selected_indices]
        X_test_selected = X_test[:, selected_indices]

        mse, r_squared = train_and_evaluate(X_train_selected, X_test_selected, y_train, y_test)
        mse_scores.append(mse)
        r_squared_scores.append(r_squared)

    # 计算平均MSE和R²
    end_time = time.time()
    average_mse = np.mean(mse_scores)
    average_r_squared = np.mean(r_squared_scores)
    
    print("平均MSE:", average_mse)
    print("平均R²:", average_r_squared)
    execution_time = end_time - start_time
    print(f"代码执行时间为: {execution_time} 秒")

if __name__ == "__main__":
    main()