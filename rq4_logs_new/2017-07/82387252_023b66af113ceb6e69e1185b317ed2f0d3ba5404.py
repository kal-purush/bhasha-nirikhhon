# -*- coding: utf-8 -*-
# author : Takuro Yamazaki
# last update : 07/06/2017
# description : エッジの数によるsignal意思決定ゲームの学習差を検証する

# データに記述すべきメタデータ
# - 繰り返し回数
# - エージェント数
# - ネットワークの種類
# - エージェントの種類
# - エッジ数
# - 利得行列の種類
# - その他の条件(初期行動制約など)

import numpy as np
import sys
import networkx as nx
sys.path.append("../")
from payoff_matrix import *

import matplotlib
matplotlib.use("Agg") #prevent no display error

import matplotlib.pyplot as plt
plt.style.use('ggplot')

import warnings
from tqdm import tqdm
warnings.filterwarnings('ignore') #warning表示なし

from agent.Q_Learning_Agent import Q_Learning_Agent
#from agent.Satisficing_Agent import Satisficing_Agent
#from agent.WoLF_PHC_Agent import WoLF_PHC_Agent
import pandas as pd

class synchro_world_signal:
    def __init__(self, n_agent, n_round, payoff_matrix):
        self.n_agent = n_agent
        self.n_round = n_round
        self.game_name, self.action_name, self.payoff_matrix = payoff_matrix
        self.create_network(n_agent)
        self.agent_action_table = pd.DataFrame(np.zeros((n_round, n_agent)))
        self.agent_payoff_table = pd.DataFrame(np.zeros((n_round, n_agent)))

    def report_meta_info(self, f_name, other=None):
        with open(f_name, "w") as f:
            f.write("繰り返し回数 : "+str(self.n_round)+"\n")
            f.write("エージェント数 : "+str(self.n_agent)+"\n")
            f.write("エッジ数 : "+str(self.n_edges)+"\n")
            f.write("ネットワーク種類 : "+self.G_alg+"\n")
            f.write("利得行列 : "+self.game_name+"\n")
            f.write("強化学習アルゴリズム : "+self.rl_alg+"\n")
            if other is not None:
                f.write("その他の条件 : "+other)

    def create_network(self, n_agent):
        """
        @description ネットワークモデルの定義
        @param n_agent エージェントの数
        """
        self.G_alg, self.G = "random", nx.random_regular_graph(d=50, n=n_agent)
        self.n_edges = 0
        self.rl_alg = "Q_Learning_Agent" #rl alg name

        for n in self.G.nodes():
            neighbors = self.G.neighbors(n)
            self.n_edges += len(neighbors)
            agent = eval(self.rl_alg)(n, sorted(neighbors), np.arange(len(neighbors)+1), self.action_name) #エージェントの定義を変えるのはここ
            self.G.node[n]["agent"] = agent
            self.G.node[n]["action"] = 0
            self.G.node[n]["n_signal"] = 0 #状態の初期値は0

    def draw_network(self, f_name=None, w_degree=False):
        """
        @description Graphの可視化
        @param f_name save file name
        @param w_degree draw graph dependent on the node degree
        """

        #行動によって色分けしてるけど別ので分類するなら変えよう
        color = ["r" if self.G.node[n]["action"] == 0 else "b" if self.G.node[n]["action"] == 1 else "g" for n in self.G.nodes()]
        if w_degree:
            d = nx.degree(self.G)
            nx.draw(self.G, pos=nx.circular_layout(self.G), nodelist=d.keys(), width=0.1, alpha=0.5, node_color=color, node_size=[20+v*5 for v in d.values()])
        else:
            nx.draw(self.G, pos=nx.circular_layout(self.G), width=0.1 ,alpha=0.5, node_color=color)

        if f_name is None: #file名指定がなければただ表示
            plt.show()
        else:
            plt.savefig(f_name)

    def run(self):
        rand = True
        for i in range(self.n_round):
            if i % 100 == 0:
                print('iter : '+str(i))
            #全エージェントが同期的に行動選択
            if i == len(self.payoff_matrix)*5: #初期のランダム行動
                rand=False
            for n in  self.G.nodes():
                self.G.node[n]["action"] = self.G.node[n]["agent"].act(self.G.node[n]["n_signal"], random=rand, reduction=True) #reduction=Trueで減衰
                self.agent_action_table[n][i] = self.G.node[n]["action"]

            #報酬計算&Q値更新
            for n in self.G.nodes():
                neighbors = self.G.node[n]["agent"].get_neighbors()
                n_action = self.G.node[n]["action"]
                n_reward = 0
                n_signal = 0
                for ne in neighbors:
                    ne_action = self.G.node[ne]["action"]
                    if ne_action==0 or ne_action==2:
                        n_signal += 1
                    n_reward += self.payoff_matrix[n_action][ne_action][0]

                self.G.node[n]["reward"] = n_reward
                self.agent_payoff_table[n][i] = n_reward/len(neighbors)
                self.G.node[n]["agent"].update_q(self.G.node[n]["n_signal"], n_reward)
                self.G.node[n]["n_signal"] = n_signal


    def save_average_reward(self, f_name):
        #write(f_name,"a")で追記
        self.agent_payoff_table.apply(lambda x:np.mean(x), axis=1).to_csv(f_name, header=False, index=False)

    def save_coop_per(self, f_name):
        self.agent_action_table.apply(lambda x:len(x[x==0])/len(x), axis=1).to_csv(f_name, header=False, index=False)

all_ = ["prisoners_dilemma_sig"]

if __name__ == "__main__":
    RESULT_DIR = "../../../results/50/"
    for n in all_:
        print(n)
        RESULT_NAME = RESULT_DIR+n+'_q_reduc_signal_random_50'
        W = synchro_world_signal(100, 10000, eval(n)()) #妥当なエージェント数はいくつか
        W.run()
        W.save_average_reward(RESULT_NAME+"_ave.csv")
        W.save_coop_per(RESULT_NAME+"_per.csv")
        W.draw_network(f_name=RESULT_NAME+"_fig.png", w_degree=True)
        W.report_meta_info(f_name=RESULT_NAME+"_meta.txt", other="シグナル")
        print('save done!!')