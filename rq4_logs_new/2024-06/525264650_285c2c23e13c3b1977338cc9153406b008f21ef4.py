# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3.8.11 (scrappy)
#     language: python
#     name: scrappy-3.8.11
# ---

# %% [markdown]
# * Comparing the stop criterion of precision is the goal with the enhanced version
# * Describing the risk version

# %%
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from scipy.stats import beta

from utils_stats import (
    #hdi_ci_limits,
    successes_failures_to_hdi_ci_limits
)

from utils_viz import (
    #plot_success_rates
    plot_vhlines_lines,
)

seed = 7

# %%
success_rate_null = 0.5   # this is the null hypothesis, not necessarilly true
dsuccess_rate = 0.05 #success_rate * 0.1
rope_precision_fraction = 0.8

success_rate = 0.5  # the true value
# --------

rope_min = success_rate_null - dsuccess_rate
rope_max = success_rate_null + dsuccess_rate

precision_goal = (2 * dsuccess_rate) * rope_precision_fraction

print(f"{success_rate_null:0.2}: null")
print(f"{rope_min:0.2}: ROPE min")
print(f"{rope_max:0.2}: ROPE max")
print("-" * 20)
print(f"{precision_goal:0.2}: Precision Goal")
print("-" * 20)
print(f"{success_rate:0.2}: true")

# %%
experiments = 50 # number of experiments 500 #200 #300 #200
n_samples = 1000 #2500 # max number of samples in each experiement #2500 #1000 #1500

np.random.seed(seed)
samples = np.random.binomial(1, success_rate, [experiments, n_samples])

samples.shape  # (experiments, n_samples)


# %%
def _update_iteration_tally(iteration_dict, iteration):
    for this_iteration in range(iteration, len(iteration_dict)+1):
        iteration_dict[this_iteration] += 1


# %%
iteration_number = np.arange(1, n_samples + 1)

iteration_pitg_accept = {iteration: 0 for iteration in range(1, n_samples + 1)}
iteration_pitg_below = iteration_pitg_accept.copy()
iteration_pitg_above = iteration_pitg_accept.copy()


iteration_epitg_accept = iteration_pitg_accept.copy()
iteration_epitg_below = iteration_epitg_accept.copy()
iteration_epitg_above = iteration_epitg_accept.copy()

method_stats = {"pitg": {}, "epitg": {}}

for isample, sample in enumerate(samples):
    pitg_stopped = False

    iteration_successes = sample.cumsum()
    iteration_failures = iteration_number - iteration_successes

    for iteration, successes, failures in zip(iteration_number, iteration_successes, iteration_failures):
        aa = int(successes)
        bb = int(failures)
        
        if not failures:
            aa += 1
            bb += 1
            
        if not successes:
            aa += 1
            bb += 1

        hdi_min, hdi_max = successes_failures_to_hdi_ci_limits(aa, bb)

        # has the precision goal been achieved?
        precision_goal_achieved = (hdi_max - hdi_min) < precision_goal

        if precision_goal_achieved:
            decision_accept = (hdi_min >= rope_min) & (hdi_max <= rope_max)
            decision_reject_below = hdi_max < rope_min  
            decision_reject_above = rope_max < hdi_min

            conclusive = decision_accept | decision_reject_above | decision_reject_above

            # update Precision Is The Goal Stop
            if pitg_stopped is False:
                # not applying `break` because we continue for ePiTG
                if decision_accept:
                    _update_iteration_tally(iteration_pitg_accept, iteration)
                elif decision_reject_below:
                    _update_iteration_tally(iteration_pitg_below, iteration)
                elif decision_reject_above:
                    _update_iteration_tally(iteration_pitg_above, iteration)

                method_stats["pitg"][isample] = {"decision_iteration": iteration,
                                                 "accept": decision_accept,
                                                    "reject_below": decision_reject_below,
                                                    "reject_above": decision_reject_above,
                                                    "conclusive": conclusive,
                                                    "inconclusive": not conclusive,
                                                    "successes": successes,
                                                    "failures": failures,
                                                    "hdi_min": hdi_min,
                                                    "hdi_max": hdi_max,
                                                 }   
                pitg_stopped = True  # sample does not continue with PITG (only ePiTG) 

            # continue with Enhance Precision Is The Goal
            if decision_accept:
                _update_iteration_tally(iteration_epitg_accept, iteration)
            elif decision_reject_below:
                _update_iteration_tally(iteration_epitg_below, iteration)
            elif decision_reject_above:
                _update_iteration_tally(iteration_epitg_above, iteration)

            if conclusive:
                method_stats["epitg"][isample] = {"decision_iteration": iteration,
                                                                "accept": decision_accept,
                                                                    "reject_below": decision_reject_below,
                                                                    "reject_above": decision_reject_above,
                                                                    "conclusive": conclusive,
                                                                    "inconclusive": not conclusive,
                                                                    "successes": successes,
                                                                    "failures": failures,
                                                                    "hdi_min": hdi_min,
                                                                    "hdi_max": hdi_max,
                                                                }  
                break


# %%
def stats_dict_to_df(method_stats):
    df = pd.DataFrame(method_stats).T
    df.index.name = "experiment_number"
    df["precision"] = df["hdi_max"] - df["hdi_min"]
    return df


df_stats_epitg = stats_dict_to_df(method_stats["epitg"])
df_stats_epitg

# %%
df_stats_pitg = stats_dict_to_df(method_stats["pitg"])
df_stats_pitg

# %%
df_stats_pitg.equals(df_stats_epitg)


# %%
def iteration_counts_to_df(dict_accept, dict_below, dict_above, experiments):
    df = pd.DataFrame({
        "iteration": list(dict_accept.keys()),
        "accept": list(dict_accept.values()),
        "reject_below": list(dict_below.values()),
        "reject_above": list(dict_above.values())
    })

    df['reject'] = df['reject_above'] + df['reject_below']
    df['inconclusive'] = experiments - df['accept'] - df['reject']

    return df

df_pitg_counts = iteration_counts_to_df(iteration_pitg_accept, iteration_pitg_below, iteration_pitg_above, experiments)
df_epitg_counts = iteration_counts_to_df(iteration_epitg_accept, iteration_epitg_below, iteration_epitg_above, experiments)

df_epitg_counts.head(4)


# %%
df_epitg_counts.describe()

# %%
df_pitg_counts.describe()

# %%
df_pitg_counts.equals(df_epitg_counts)

# %%
title = f"true success rate = {success_rate:0.2f}"
xlabel = "iteration"

iteration_values = df_pitg_counts["iteration"]

# plotting pitg
alpha, linewidth = 0.4, 5
plt.plot(iteration_values, df_pitg_counts['accept'] / experiments, color="green", linewidth=linewidth, alpha=alpha)
plt.plot(iteration_values, df_pitg_counts['reject'] / experiments, color="red", linewidth=linewidth, alpha=alpha)
plt.plot(iteration_values, df_pitg_counts['inconclusive'] / experiments, color="gray", linewidth=linewidth, alpha=alpha)

# plotting epitg
alpha, linewidth, linestyle = 0.7, 3, "--"
plt.plot(iteration_values, df_epitg_counts['accept'] / experiments, color="green", linewidth=linewidth, alpha=alpha, linestyle=linestyle)
plt.plot(iteration_values, df_epitg_counts['reject'] / experiments, color="red", linewidth=linewidth, alpha=alpha, linestyle=linestyle)
plt.plot(iteration_values, df_epitg_counts['inconclusive'] / experiments, color="gray", linewidth=linewidth, alpha=alpha, linestyle=linestyle)


#plt.xscale('log')
#plt.yscale('log')
plt.xlabel(xlabel)
plt.ylabel(f"proportion of {experiments:,} experiments")
plt.title(title)


# %%
_, bins = np.histogram(np.concatenate([df_stats_epitg["decision_iteration"], df_stats_pitg["decision_iteration"]]), bins=100)

plt.hist(df_stats_epitg["decision_iteration"], bins=bins, histtype='step', label="PitG")
plt.hist(df_stats_pitg["decision_iteration"], bins=bins, histtype='step', label="ePitG")   
plt.xlabel("stop iteration")
plt.ylabel("number of experiments")
plt.legend()
pass

# %%
#decision_iteration = df_stats_pitg["decision_iteration"].value_counts().idxmax()
#sr_experiment_stats = df_stats_pitg.query("decision_iteration == @decision_iteration").iloc[0]

idx = list(sorted(set(df_stats_pitg.index) - set(df_stats_epitg.index)))[1]
sr_experiment_stats = df_stats_pitg.loc[idx]

pp = np.linspace(0, 1, 1000)
pp_hdi = np.linspace(sr_experiment_stats["hdi_min"], sr_experiment_stats["hdi_max"], 1000)

successes = sr_experiment_stats["successes"]
failures = sr_experiment_stats["failures"]
rate = successes / (successes + failures)
n_ = successes + failures

pdf = beta.pdf(pp, successes, failures)
pdf_hdi = beta.pdf(pp_hdi, successes, failures)

plt.plot(pp, pdf, color="purple", label=f"pdf p={rate:0.2f}; n={n_:,}")
plt.fill_between(pp_hdi, pdf_hdi, color="purple", alpha=0.2, label="HDI")
plot_vhlines_lines(vertical=rope_min, label='ROPE', horizontal=None)
plot_vhlines_lines(vertical=rope_max, horizontal=None)
plt.legend()
plt.xlabel("success rate")
plt.ylabel("probability density")

plt.xlim([rope_min - 0.1, rope_max + 0.1])

# %%