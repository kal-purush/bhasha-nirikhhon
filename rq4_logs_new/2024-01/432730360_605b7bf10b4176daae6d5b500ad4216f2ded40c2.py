import sys
import polars as pl
from pathlib import Path
#import matplotlib.pyplot as plt
#import seaborn as sns

import textgrid

grid_file = Path.home() / \
    'Sounds/WhiteHousePressBriefings/data2/mfa_align/11⧸20⧸23： Press Briefing by Press Secretary Karine Jean-Pierre [FYZztiGyz4g].TextGrid'

dat = textgrid.read(grid_file)
print(dat.columns)

# Combine word and phone tiers.
dat_combo = textgrid.combine_tiers(dat)
print(dat_combo)
print(dat_combo.columns)

# Tokens of 'the'.
dat_the = dat.filter( \
    pl.col('tier') == 'words',
    pl.col('label') == 'the')
print(dat_the)

# Preceding words.
dat_prev = textgrid.preceding(dat_the, dat)

# Following words.
dat_next = textgrid.following(dat_the, dat)
print(dat_next)

dat_the = dat_the.with_columns( \
    word_prev=dat_prev['label'],
    word_dur_ms_prev=dat_prev['dur_ms'],
    word_next=dat_next['label'],
    word_dur_ms_next=dat_next['dur_ms'])
print(dat_the)
print(dat_the.columns)

# Speaking rate before each 'the'.
dat_the = textgrid.speaking_rate( \
    dat_the, dat, side='before')

print(dat_the)
print(dat_the.columns)
print(len(dat_the))

# Merge with combo.
dat_the = dat_the \
    .rename({'label': 'word', 'start': 'word_start', 'end': 'word_end', 'dur_ms': 'word_dur_ms'}) \
    .drop('tier')

print(dat_the)
print(dat_the.columns)

dat_the = dat_the.join( \
    dat_combo,
    on = ['filename', 'speaker', 'word', 'word_start', 'word_end', 'word_dur_ms'])

print(dat_the)
print(dat_the.columns)
print(len(dat_the))

print(dat_the[['word', 'word_prev', 'word_next', 'phone']].tail())

print(dat_the.filter(pl.col('word_prev') == 'of'))

sys.exit(0)