#!/usr/bin/env python3
import sys
import math
import random
from scipy.optimize import minimize


class EV(object) :
    def __init__(self, id, kind = None):
        self._id = id                          # identification number of the EV group        

        if kind == None:  self.set_kind()      # kind をランダムに設定
        else:             self._kind  = kind   # kind を指定して設定

        self._total_E_kWh = 0                  # 現在の電力売買量
        self._total_P_yen = 0                  # 現在の電力売買価格

        self._EVA_can = None                   # 行き先候補（辞書型）{id:dis, id:dis ・・ }
        self._EVA_id = None                    # 電力売買する EVA
     

    def set_kind(self) -> str:
        k = random.random()
        if k < 0.5:  kind = "S"
        else:        kind = "B"

        self._kind = kind


    def set_param_time(self, arrT, depT, nowT) -> None:
        self._arrT = arrT      # arrival time of EV
        self._depT = depT      # departure time of EV
        self._nowT = nowT      # now time of EV
        self._preT = None      # リクエストした電力量がやり取りできるまでの推定時間


    def set_param_model(self, kappa_dis, kappa_P, kappa_nu):
        self._kappa_dis = kappa_dis    # 多項ロジットモデルでの距離の重み
        self._kappa_nu  = kappa_nu     # 多項ロジットモデルでのランダム性の強さ
        if self._kind == "S":
            self._kappa_P = -kappa_P      # 多項ロジットモデルでの価格の重み
        elif self._kind == "B":
            self._kappa_P =  kappa_P      


    def set_param_calc(self, alpha):
        self._alpha   = alpha         # parameter of the utility function        
        self._gamma_r = 0.1           # coefficient used in the derivs            
        self._gamma_P = 200           # 電力価格の計算式における係数
        self._gamma_fact = 2          # 価格の計算式に用いられているガンマ
        if self._kind == "S":
            self._base_P = 20         # 電力の基準となる価格を設定
        elif self._kind == "B":
            self._base_P = 25      
        
        
    def set_param_private(self, EVA_can, home_id, E):
        self._EVA_can = EVA_can         # 候補の EVAid と距離の集合（辞書型）
        self._home_id = home_id         # 出発地点（ウッディータウンかフラワータウンの人かを判別）
        self._request_E_kWh = E         # EV ユーザが取引したい電力量

        
    def derivs_util(self, x):
        return (x + 1)**(-self._alpha)   # differential equation of the utility function

    
    def derivs_x(self, x, p):
        return self._gamma_r*(self.derivs_util(x) - p*x)  # differential equation of the rate


    def set_P_now(self, p, nextT_h, eva) -> None:
        # 前回計算してからの経過時間
        passedT = nextT_h - self._nowT

        # 売り手と買い手によって異なる計算式
        if self._kind == "S":
            tmp_p = -(p[0] - p[2])

        elif self._kind == "B":
            tmp_p =   p[1] + p[2]

        # 価格計算 (1kWh あたりの電力価格)
        p = self._gamma_P * self._E_rate_kW**(self._gamma_fact) * tmp_p
        P_rate = (self._E_rate_kW*self._base_P + p) / self._E_rate_kW
        self._P_rate = eva.adjust_price(P_rate)
        

    def set_total_P_E(self, nextT_h) -> None:
        # 今まで取引した料金および電力量
        self._total_P_yen += self._P_rate*self._E_rate_kW*(nextT_h - self._nowT)
        self._total_E_kWh += self._E_rate_kW*(nextT_h - self._nowT)


    def calc_depT(self):
        # リクエストした電力量のやり取りが終わるまでの時間を推定
        self._preT = self._nowT + (self._request_E_kWh - self._total_E_kWh)/self._E_rate_kW
        
        


        
class EVA(object):
    def __init__(self, id):
        self._id = id
        self._EV_list = {}

        # EV の種類別にカウント
        self._S_EV_num = 0
        self._B_EV_num = 0

        # EVA 内の台数が上限のとき → True
        # EVA 内の台数が上限でないとき → False 
        self._is_limit = False
        
        # V 2 バッテリー → True
        # V 2 V          → False
        self._battery_exists = False
        # バッテリー容量  ※初期値は半分とする
        self._battery_kWh = 1000/2


    def set_param(self, limit_As, limit_Ar) -> None:
        # EVA k でのペナルティ
        self._k1 = 0.1                
        self._k2 = 0.1
        self._k3 = 0.1

        self._init_x = 1.0
        self._init_p = 0.0

        # ケーブル容量
        self._limit_As = limit_As      
        self._limit_Ar = limit_Ar

        
    def set_param_value(self, pre_P, EV_limit_num) -> None:
        self._preP = pre_P
        self._EV_limit_num = EV_limit_num

            
    def add_EV(self, ev) -> None:
        self._EV_list[ev._id] = ev
        self.EV_kind_counter(kind = ev._kind, count = 1)
        
        # バッテリーで電力売買している場合
        if self._battery_exists:
            # 電力売買する EV が見つかった場合にバッテリーを取り除く
            if self._EV_list[0]._kind == ev._kind:
                self.remove_battery()
        else:
            # 訪れた EV の電力売買を可能にするためバッテリーを追加
            if self._S_EV_num == 0:
                self.generate_battery(ev, kind = "S")
            elif self._B_EV_num == 0:
                self.generate_battery(ev, kind = "B")
            

    def remove_EV(self, ev) -> None:
        # dep_list を作成するため，popを使用し，出発する EV を返す
        dep_ev = self._EV_list.pop(ev._id)
        self.EV_kind_counter(kind = dep_ev._kind, count = -1)
        
        # バッテリーで電力売買している場合
        if self._battery_exists:
            # バッテリーだけが EVA 内にいるので取り除く
            if len(self._EV_list) == 1:                
                self.remove_battery()
        else:
            # 訪れた EV の電力売買を可能にするため EVA を追加
            if self._S_EV_num == 0:
                self.generate_battery(ev, kind = "S")
            elif self._B_EV_num == 0:
                self.generate_battery(ev, kind = "B")
                
        return dep_ev
        
    
    def generate_battery(self, ev, kind):
        eva_battery = EV(id = 0, kind = kind)
        eva_battery.set_param_time(arrT = ev._arrT, depT = 100, nowT = ev._nowT)
        eva_battery.set_param_calc(alpha = 2)
        eva_battery._request_E_kWh = 500
        self._EV_list[eva_battery._id] = eva_battery
        self._battery_exists = True

        
    def remove_battery(self):
        eva_battery = self._EV_list.pop(0)
        self._battery_exists = False


    def set_battery_now(self, eva_battery, time):
        if eva_battery._kind == "S":
            self._battery_kWh -= eva_battery._E_rate_kW*(time - eva_battery._nowT)
        elif eva_battery._kind == "B":
            self._battery_kWh += eva_battery._E_rate_kW*(time - eva_battery._nowT) 

            
    def EV_kind_counter(self, kind, count):
        if kind == "S":
            self._S_EV_num += count
        elif kind == "B":
            self._B_EV_num += count

        # EV の数が上限に達しているかを判断する
        total_EV_num = self._S_EV_num + self._B_EV_num
        if self._EV_limit_num <= total_EV_num:
            self._is_limit = True
        else:
            self._is_limit = False

            
    def adjust_price(self, price):
        if self._battery_exists:
           battery = self._EV_list[0]
           # 買い手EVの価格をあげる
           if battery._kind == "S":
               return price + self._preP
           # 売り手EVの価格をあげる
           elif battery._kind == "B":
               return price - self._preP
        else:
            return price
                
               
    def derivs_p1(self, As) :
        return self._k1*(As - self._limit_As)
    
    def derivs_p2(self, Ar) :
        return self._k2*(Ar - self._limit_Ar)

    def derivs_p3(self, As, Ar) :
        return self._k3*(Ar - As)
    
    def derivs_p(self, As, Ar, p1, p2, p3) -> list:
        dp1 = self.derivs_p1(As)
        dp2 = self.derivs_p2(Ar)
        dp3 = self.derivs_p3(As, Ar)

        return [dp1, dp2, dp3]
    
            
    def get_conv(self, time) -> None:
        EV_n = len(self._EV_list)
        self._init = [self._init_x]*EV_n + [self._init_p]*3
        self._opt = minimize(self.norm_derivs, self._init)   # その時のレートとペナルティを計算
        
        p1 = self._opt.x[EV_n]
        p2 = self._opt.x[EV_n + 1]
        p3 = self._opt.x[EV_n + 2]

        # 各 EV の処理
        EV_id = 0
        for ev in self._EV_list.values():
            ev._E_rate_kW = self._opt.x[EV_id]   # 新しく計算された電力レートをセット
            ev.set_P_now((p1, p2, p3), time, self)     # 現在までの電力売買価格を計算
            ev.set_total_P_E(time)               # 現在までの電力売買価格を計算
            EV_id += 1

        # バッテリーの処理
        if self._battery_exists:
            eva_battery = self._EV_list[0]
            self.set_battery_now(eva_battery, time)

        # 時間設定
        for ev in self._EV_list.values():
            ev._nowT = time
            ev.calc_depT()            

            
    def get_P_rate(self, ev) -> None:
        EV_n = len(self._EV_list)
        self._init = [self._init_x]*EV_n + [self._init_p]*3
        self._opt = minimize(self.norm_derivs, self._init)   # その時のレートとペナルティを計算
        
        p1 = self._opt.x[EV_n]
        p2 = self._opt.x[EV_n + 1]
        p3 = self._opt.x[EV_n + 2]
      
        if ev._kind == "S":
            tmp_p = -(p1 - p3)
        elif ev._kind == "B":
            tmp_p =   p2 + p3

        # 新しく計算された電力レートをセット
        E_rate_kW = self._opt.x[EV_n - 1]
        p = ev._gamma_P * E_rate_kW**(ev._gamma_fact) * tmp_p
        P_rate = (E_rate_kW*ev._base_P + p) / E_rate_kW

        # 価格を調整
        P_rate = self.adjust_price(P_rate)
   
        return P_rate


    def derivs(self, t, y):
        """
           calculate
        """ 
        ## calculate the both aggregate rates of sellers and buyers.
        As = Ar = 0.0
        i = 0
        for ev in self._EV_list.values():
            if ev._kind == "S":    As += y[i]
            elif ev._kind == "B":  Ar += y[i]
            else :                assert(False)
        
            i += 1
            
        ## get price
        p1 = y[i]
        p2 = y[i+1]
        p3 = y[i+2]

        ## calculate the derivs of stats
        dy = []
        i = 0
        for ev in self._EV_list.values():
            x = y[i]
            if ev._kind == "S":    p = p1 - p3
            elif ev._kind == "B":  p = p2 + p3
            else :                assert(False)

            dy.append(ev.derivs_x(x, p))
            i += 1

        dy = dy + self.derivs_p(As, Ar, p1, p2, p3)
        
        return dy


    def norm_derivs(self, y) :
        """
        calculate the normalized norm of the derivs vector 
           ** This norm is used to obtain the convergenced value of state

        Args :
           y (list of float) : states
        Returns :
           float : normalized norm of the derivs vector  [[dy]]_2/[[y]_2
        """
        t = 0  # dummy
        dy = self.derivs(t, y)

        return calc_norm(dy)/calc_norm(y)


    
    
class Simulator(object):
    def __init__(self, stT, endT, seed):
        self._stT = stT
        self._endT = endT

        self._EVA_list = []
        self._dep_ev_list = []
        self._cant_trade_EV_list = []
        self._EVA_candidate_list = []

        self._opt = None               # convergence values of states
        random.seed(seed)


    def set_lambda(self, lam):
        self._lam = lam


    def add_EVA(self, EVA) -> None:
        self._EVA_list.append(EVA)

        
    def rnd_exp(self):
        u = random.random()
        x = (-1/self._lam)*math.log(1 - u)

        return x

    
    def calc_all_EVA(self, time):
        for eva in self._EVA_list:
            if len(self._EVA_list[eva._id]._EV_list) > 0:
                self._EVA_list[eva._id].get_conv(time)

               
    def process_dep_EV(self, ev, t) -> None:
        # 出発するEVの出発処理
        ev._depT = t
        dep_ev = self._EVA_list[ev._EVA_id].remove_EV(ev)
        self._dep_ev_list.append(dep_ev)
        

    def select_EVA_and_add_EV(self, ev):
        # todo: 機能を追加したため，コードがぐちゃぐちゃになっているので，直す．
        # 行き先候補の EVA の電力レートを仮計算する．
        v_list = []
        for eva_id, eva_dis in ev._EVA_can.items():
            eva = self._EVA_list[eva_id]
            
            # EVAが満であれば，計算しない
            if eva._is_limit:
                v_cal = None
            else:
                eva.add_EV(ev)
                P_rate = eva.get_P_rate(ev)
                # 多項ロジットモデル
                v_cal = ev._kappa_dis*eva_dis + ev._kappa_P*P_rate
                eva.remove_EV(ev)
                
            v_list.append(v_cal)
            
        # Gumbel distribution
        all_v = 0
        for i in v_list:
            if i == None:
                all_v += 0
            else:
                all_v += math.exp(-ev._kappa_nu*i)                           
                
        pr = []
        for i in v_list:
            if i == None:
                pr.append(0)
            else:
                pr.append(math.exp(-ev._kappa_nu*i) / all_v)

                
        # random.choices を使用するため，リストに変換        
        EVA_can_list = [EVA_id for EVA_id in ev._EVA_can]

        
        # 各 EVA の効用をもとにランダムに一つの EVA を選択し，追加する
        if all(e == 0 for e in pr):
            self._cant_trade_EV_list.append(ev)
        else:
            select_EVA_id = random.choices(EVA_can_list, k = 1, weights = pr)
            ev._EVA_id = select_EVA_id[0]
            self._EVA_list[ev._EVA_id].add_EV(ev)
                
            
    def get_next_dep_EV(self) -> object:
        # 次に出発する EV を選択
        next_dep_ev = EV(id = 0)  # ダミー ev の作成
        next_depT = 1000
        for eva in self._EVA_list:     
            for ev in eva._EV_list.values():                
                if ev._depT < ev._preT:
                    depT = ev._depT
                else:
                    depT = ev._preT
                    
                if depT < next_depT:
                    next_dep_ev = ev
                    next_depT = depT
        
        return next_dep_ev, next_depT
    
            
    def read_data_from_file(self, file, trade_p) -> None:
        # EV の情報が入ったファイルの読み込み (下記の形式で保存)
        # kind req_E home_id EVA_id_1 EVA_id_2 EVA_dis_1 EVA_dis_2
        self._kind_list = []
        self._req_E_list = []
        self._home_list = []
       
        for line in open(file, "r"):
            EVA_data_tmp = {}
            data = list(line.split())

            j = int((len(data)-3) / 2)

            self._kind_list.append(data[0])
            self._req_E_list.append(float(data[1])*trade_p)
            self._home_list.append(int(data[2]))
            
            for i in range(j):
                EVA_data_tmp.update({int(data[i+3]): int(data[j+i+3])})
                
            self._EVA_candidate_list.append(EVA_data_tmp)


    def EV_info_generator(self):
        for EV_kind, req_E, dep_id, EVA_can in zip(self._kind_list,
                                                   self._req_E_list,
                                                   self._home_list,
                                                   self._EVA_candidate_list):
            
            yield EV_kind, req_E,  dep_id, EVA_can


    def simulation(self):
        EV_id = 0
        t = self._stT                 # current time
        next_arrT = self.rnd_exp()    # next arrive time
        next_depT = self._endT        # next departure time
        EV_info_g = self.EV_info_generator()

        while (t < self._endT):
            if (next_arrT < next_depT):
                # 到着時刻 t までの電力売買を完了させる．
                t = next_arrT
                self.calc_all_EVA(t)

                # EV を生成し，売買する EVA を選択
                ev = make_EV(id = EV_id + 1, arrT = t, depT = t + 0.5,
                             k_dis = 0.03, k_P = kappa_P, k_nu = 0.5,
                             EV_info_g = EV_info_g)
                self.select_EVA_and_add_EV(ev)                      
                EV_id += 1

                # 次に到着する EV の時刻を格納
                next_arrT += self.rnd_exp()

            else:
                # 出発時刻 t までの電力売買を完了させる．
                t = next_depT            
                self.calc_all_EVA(t)

                # 出発する EV の処理                    
                self.process_dep_EV(next_dep_EV, t)   

            self.calc_all_EVA(t)
            # 次に出発する (EV, EV の時刻) を格納          
            next_dep_EV, next_depT = self.get_next_dep_EV()
            self.print_time_EVnum(t)


            
    def print_time_EVnum(self, t) -> None:
        for eva in self._EVA_list:
            print("EVA_Time: EVA {0} time {1} S_num {2} Bnum {3}"
                  .format(eva._id, t, eva._S_EV_num, eva._B_EV_num))
        
        
    def print_dep_list(self) -> None:
        print()
        for ev in self._dep_ev_list:
            print("EV_INFO: id {1} kind {2} EVA {0} arrT {3} depT {4} totalT {5} request_E {6} E_T {7} P_T {8}"
                  .format(ev._EVA_id, ev._id, ev._kind, ev._arrT, ev._depT,
                          ev._depT - ev._arrT, round(ev._request_E_kWh, 3),
                          round(ev._total_E_kWh, 3), round(ev._total_P_yen, 3)))

            
    def print_trade_EVA(self):
        # 各 EVA での総電力売買量を S と B を分けて計算
        eva_S_total_trade_kWh = [0]*len(self._EVA_list)
        eva_B_total_trade_kWh = [0]*len(self._EVA_list)
        for ev in self._dep_ev_list:
            if ev._kind == "S":
                eva_S_total_trade_kWh[ev._EVA_id] += ev._total_E_kWh
            if ev._kind == "B":
                eva_B_total_trade_kWh[ev._EVA_id] += ev._total_E_kWh

        # 各 EVA での V2G
        eva_V2G_trade_kWh = [0]*len(self._EVA_list)
        for eva in self._EVA_list:
            # バッテリーの初期値が 500 kWh
            eva_V2G_trade_kWh[eva._id] = eva._battery_kWh - 500

        print()
        eva_S_V2V_trade_kWh = [0]*len(self._EVA_list)
        eva_B_V2V_trade_kWh = [0]*len(self._EVA_list)
        for eva in self._EVA_list:
            if eva_V2G_trade_kWh[eva._id] < 0:                                 
                eva_S_V2V_trade_kWh[eva._id] = eva_S_total_trade_kWh[eva._id]
                eva_B_V2V_trade_kWh[eva._id] = eva_B_total_trade_kWh[eva._id] + eva_V2G_trade_kWh[eva._id] 
            else:
                eva_S_V2V_trade_kWh[eva._id] = eva_S_total_trade_kWh[eva._id] - eva_V2G_trade_kWh[eva._id]                                                                  
                eva_B_V2V_trade_kWh[eva._id] = eva_B_total_trade_kWh[eva._id] 
                
            print("EVA_INFO: EVA {0} S_total {1} B_total {2} V2G {3} S_V2V {4} B_V2V {5}"
                  .format(eva._id, eva_S_total_trade_kWh[eva._id],
                          eva_B_total_trade_kWh[eva._id], eva_V2G_trade_kWh[eva._id],
                          eva_S_V2V_trade_kWh[eva._id], eva_B_V2V_trade_kWh[eva._id]))
            
                        

            
def calc_norm(vec) :
    """
      calculate the 2nd norm of vector
    """
    norm = 0.0
    for v in vec:
        norm += v*v
        
    return math.sqrt(norm)


def make_EV(id, arrT, depT, k_dis, k_P, k_nu, EV_info_g):
    EV_kind, req_E, home_id, EVA_can = EV_info_g.__next__()
    
    ev = EV(id = id, kind = EV_kind)
    ev.set_param_calc(alpha = 2)
    ev.set_param_time(arrT = arrT, depT = depT, nowT = arrT)
    ev.set_param_model(kappa_dis = k_dis, kappa_P = k_P, kappa_nu = k_nu)
    ev.set_param_private(EVA_can = EVA_can, home_id = home_id, E = req_E)

    return ev


def calc_lambda(usage_p, time_h):
    # 以下の情報から一時間あたりの到着台数 lam を計算

    fre_p = 0.3                 # 使用頻度
    usage_p = usage_p           # 普及率
    total_house_holds = 13000   # ウッディータウンの世帯数

    lam = total_house_holds*usage_p*fre_p / time_h

    return lam
    
    
    
if __name__ == "__main__":
    # parameter setting for simulation
    seed = int(sys.argv[1])
    EVA_num = int(sys.argv[2])              # EVA の台数
    kappa_P = float(sys.argv[3])            # 多項ロジットモデルでの価格の重要性
    usage_p = float(sys.argv[4])            # EV の普及率
    trade_p = float(sys.argv[5])            # 売買量の割合
    EV_limit_num = int(sys.argv[6])         # EVA が同時に充電できる最大の数    
    limit_As = limit_Ar = int(sys.argv[7])  # ケーブル容量
    candidate_file = sys.argv[8]            # EV の情報が書き込まれたファイル

    # make simulator
    stT = 0         
    endT = 12        
    sim = Simulator(stT = stT, endT = endT, seed = seed)

    # make EVA
    for i in range(EVA_num):
        eva = EVA(id = i)
        eva.set_param(limit_As = limit_As, limit_Ar = limit_Ar)
        eva.set_param_value(pre_P = 1.0, EV_limit_num = EV_limit_num)
        sim.add_EVA(eva)

    # preparation for simulation 
    sim.read_data_from_file(candidate_file, trade_p)
    lam = calc_lambda(usage_p, endT)
    sim.set_lambda(lam)
    random.seed(seed)

    # simulation
    sim.simulation()
    #sim.check_simulation()
    sim.print_dep_list()
    sim.print_trade_EVA()    