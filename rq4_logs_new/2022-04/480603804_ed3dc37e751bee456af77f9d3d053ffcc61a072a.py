"""
Load and analyze match data
author: bradicator
created on: 2022/04/15
"""

import pandas as _pd
from itertools import product as _product
from collections import defaultdict as _ddict

def load_matches(fpath, turbo_only=True):
    colnames = ['match_id', 'duration', 'game_mode',
                            'human_players', 'start_time', 'version',
                            'patch', 'radiant_win', 'hero0', 'hero1',
                            'hero2', 'hero3', 'hero4', 'hero5',
                            'hero6', 'hero7', 'hero8', 'hero9']
    if not isinstance(fpath, list):
        fpath = [fpath]
    df = _pd.DataFrame([], columns=colnames)
    for f in fpath:
        df_batch = _pd.read_csv(f)
        df_batch.columns = colnames
        df = df.append(df_batch)
    if turbo_only:
        df = df.loc[df.game_mode == 23, :]
    return df

def get_match_up_count_table(df):
    res = []
    for idx, r in df.iterrows():
        for i, j in _product(range(5), range(5, 10)):
            if r.radiant_win:
                res.append((r[f'hero{i}'], r[f'hero{j}'], 1))
            else:
                res.append((r[f'hero{j}'], r[f'hero{i}'], 1))
    raw_table = _pd.DataFrame(res, columns=['winhero', 'losehero', 'nwin'])
    return raw_table.groupby(['winhero', 'losehero'])['nwin'].count().reset_index()

def get_hero_winrate(heroid, muctable):
    nwin = muctable.loc[muctable.winhero == heroid, :].shape[0]
    nloss = muctable.loc[muctable.losehero == heroid, :].shape[0]
    return nwin / (nwin+nloss), nwin, nloss

def get_counter_winrate(heroid, muctable):
    avgwinrate = get_hero_winrate(heroid, muctable)[0]
    herowindict = _ddict(int)
    herolossdict = _ddict(int)
    dfwin = muctable.loc[muctable.winhero == heroid, ['losehero', 'nwin']]
    dfloss = muctable.loc[muctable.losehero == heroid, ['winhero', 'nwin']]
    for idx, r in dfwin.iterrows():
        herowindict[r.losehero] = r.nwin
    for idx, r in dfloss.iterrows():
        herolossdict[r.winhero] = r.nwin
    res = []
    for hid in range(150):
        if herowindict[hid] > 0 or herolossdict[hid] > 0:
            wr = herowindict[hid] / (herowindict[hid] + herolossdict[hid])
            res.append((hid, herowindict[hid], herolossdict[hid], wr, avgwinrate))
    df = _pd.DataFrame(res, columns=['against_hero', 'nwin', 'nloss', 
                                     'matchup_winrate', 'avg_winrate'])
    return df.sort_values('matchup_winrate')
    