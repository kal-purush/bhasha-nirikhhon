import pandas as pd
from tqdm import tqdm
import numpy as np
import json
import ast

def get_df(file):
    df = pd.read_csv(file)
    df = df.loc[:,["ResponseId", "round_1_actions", "round_2_actions", "round_3_actions", 
                    "round_4_actions", "round_5_actions", "round_6_actions"]]
    df = df.dropna().reset_index().iloc[:,1:]
    return df

def player_round12(ticks):
    tick_array = []
    time = 1
    worker_this_round = []
    order_this_round = []
    task_this_round = []
    action_this_tick = []
    for tick in ticks:
        inn = False
        worker_this_tick = {0:1, 1:1, 2:2} #key is worker
        order_this_tick = {0:6, 1:6, 2:6} #key is worker
        task_this_tick = {0:6, 1:6, 2:6} #key is worker
        for action in tick:
            if list(action.values()) not in action_this_tick:
                #print(time, "action: ", action, "values: ", list(action.values()), "list: ", action_this_tick)
                worker = action["worker_index"]
                worker_this_tick[worker] = worker
                order_this_tick[worker] = action["order_index"]
                task_this_tick[worker] = action["task"]
                action_this_tick.append(list(action.values()))
            # else:
                #print(time, "action: ", action, "values: ", list(action.values()), "list: ", action_this_tick)
        time_this_tick = [time for _ in np.arange(3)]
        tick_array.extend(time_this_tick)
        time += 1
        worker_this_round.extend(list(worker_this_tick.values()))
        order_this_round.extend(list(order_this_tick.values()))
        task_this_round.extend(list(task_this_tick.values()))
    tick_array, worker_this_round, order_this_round, task_this_round = np.array(tick_array), np.array(worker_this_round), np.array(order_this_round), np.array(task_this_round)
    out = pd.DataFrame()
    out["tick"], out["worker"], out["order"], out["task"] = tick_array, worker_this_round, order_this_round, task_this_round
    return out

def get_action12(df):
    action_df_12 = pd.DataFrame()
    for id in tqdm(np.arange(len(df["ResponseId"]))):
        for round in ["round_1_actions", "round_2_actions"]:
            # print(f"generating {round}")
            df_round = player_round12(json.loads(df[round][id]))
            length = len(df_round.iloc[:,1])
            ResponseId = np.array([df.iloc[id, 0] for _ in np.arange(length)])
            intervention = np.array([df.iloc[id, 1] for _ in np.arange(length)])
            round_array = np.array([round.split("_")[1] for _ in np.arange(length)])
            df_round["ResponseId"] = ResponseId
            df_round["intervention"] = intervention
            df_round["round"] = round_array
            df_round = df_round[["ResponseId", "intervention", "round", "tick", "worker",  "order", "task"]]
            action_df_12 = pd.concat([action_df_12, df_round])

    return action_df_12

def player_round36(ticks):
    tick_array = []
    time = 1
    worker_this_round = []
    order_this_round = []
    task_this_round = []
    action_this_tick = []
    for tick in ticks:
        inn = False
        worker_this_tick = {1:1, 2:2} #key is worker
        order_this_tick = {1:6, 2:6} #key is worker
        task_this_tick = {1:6, 2:6} #key is worker
        for action in tick:
            if list(action.items()) not in action_this_tick:
                worker = action["worker_index"] + 1
                worker_this_tick[worker] = worker
                order_this_tick[worker] = action["order_index"]
                task_this_tick[worker] = action["task"]
                action_this_tick.append(list(action.items()))        
        time_this_tick = [time for _ in np.arange(2)]
        tick_array.extend(time_this_tick)
        time += 1
        worker_this_round.extend(list(worker_this_tick.values()))
        order_this_round.extend(list(order_this_tick.values()))
        task_this_round.extend(list(task_this_tick.values()))
    tick_array, worker_this_round, order_this_round, task_this_round = np.array(tick_array), np.array(worker_this_round), np.array(order_this_round), np.array(task_this_round)
    #print(tick_array)
    out = pd.DataFrame()
    out["tick"], out["worker"], out["order"], out["task"] = tick_array, worker_this_round, order_this_round, task_this_round
    return out

def get_action36(df):
    action_df_36 = pd.DataFrame()
    for id in tqdm(np.arange(len(df["ResponseId"]))):
        for round in ["round_3_actions", "round_4_actions", "round_5_actions", "round_6_actions"]:
            # print(f"generating {round}")
            df_round = player_round36(json.loads(df[round][id]))
            length = len(df_round.iloc[:,1])
            ResponseId = [df.iloc[id, 0] for _ in np.arange(length)]
            intervention = [df.iloc[id, 1] for _ in np.arange(length)]
            round_array = [round.split("_")[1] for _ in np.arange(length)]
            df_round["ResponseId"] = ResponseId
            df_round["intervention"] = intervention
            df_round["round"] = round_array
            df_round = df_round[["ResponseId", "intervention", "round", "tick", "worker",  "order", "task"]]
            action_df_36 = pd.concat([action_df_36, df_round])
    return action_df_36

def get_summary(action_df):
    tem = action_df.loc[action_df.task != 6,:]
    pivoting = pd.pivot_table(tem, values="order", index=["ResponseId", "round", "worker"],
                        columns=["task"], aggfunc=len)
    # pivoting = pivoting.rename(columns = {"intervention":  "total assignment frequency"})
    unstack = pivoting.unstack("worker")
    final_action_df = tem.merge(unstack, how = "left", left_on = ["ResponseId", "round"], right_index=True)
    final_action_df = final_action_df.rename(columns = 
                                            {(0, 0): "chef_round_chopping", (1, 0): "chef_round_cooking", (2, 0): "chef_round_plating",
                                            (0, 1): "souschef_round_chopping", (1, 1): "souschef_round_cooking", (2, 1): "souschef_round_plating",
                                            (0, 2): "server_round_chopping", (1, 2): "server_round_cooking", (2, 2): "server_round_plating",
                                            })
    final_action_df = final_action_df.fillna(0)
    final_action_df
    return  final_action_df

def get_action(args):
    df = get_df(args.file)
    action_df_12, action_df_36 = get_action12(df), get_action36(df)
    action_df = pd.concat([action_df_12, action_df_36])
    action_summary = get_summary(action_df)
    return action_df, action_summary