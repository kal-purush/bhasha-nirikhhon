from typing import Callable, List, Union

import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.stats.power import TTestIndPower


def lower_bound(
    alpha: float, beta: float, means_diff: float, std: float, two_tail: bool = True
):
    """
    only for normal distributed values
    alpha -- type I error
    beta -- type II error
    returns lower bound of size for each group
    https://rugg2.github.io/AB%20testing%20-%20a%20simple%20explanation%20of%20what%20power%20analysis%20does.html
    """
    if two_tail:
        inv_alpha = stats.norm.ppf(1 - alpha / 2, loc=0, scale=1)
        inv_beta = stats.norm.ppf(1 - beta, loc=0, scale=1)
        return 2 * ((inv_alpha + inv_beta) / (means_diff / std)) ** 2
    else:
        inv_alpha = stats.norm.ppf(1 - alpha, loc=0, scale=1)
        inv_beta = stats.norm.ppf(1 - beta, loc=0, scale=1)
        return ((inv_alpha + inv_beta) / (means_diff / std)) ** 2


def lower_bound_statsmodels(alpha: float, beta: float, means_diff: float, std: float):
    """
    only for normal distributed values
    alpha -- type I error
    beta -- type II error
    """
    effect = (means_diff) / std
    analysis = TTestIndPower()
    result = analysis.solve_power(
        effect, power=1 - beta, nobs1=None, ratio=1.0, alpha=alpha
    )

    return result


def permutation_shares(pos_a: int, total_a: int, pos_b: int, total_b: int):
    if pos_a == 0 and pos_b == 0:
        res = 0
    else:
        if pos_a * total_b <= pos_b * total_a:
            res = stats.hypergeom.cdf(
                pos_b - 1, total_a + total_b, pos_a + pos_b, total_b
            )
        else:
            res = 1 - stats.hypergeom.cdf(
                pos_b, total_a + total_b, pos_a + pos_b, total_b
            )

    return res


def permutation(group_a, group_b, func: Callable = np.mean, num_permutations=1000):
    concatenated = np.concatenate([group_a, group_b])
    observed_stat = func(group_b) - func(group_a)

    permuted_stats = []

    for _ in range(num_permutations):
        np.random.shuffle(concatenated)
        perm_group_1 = concatenated[: len(group_a)]
        perm_group_2 = concatenated[len(group_a) :]
        permuted_stat = func(perm_group_2) - func(perm_group_1)
        permuted_stats.append(permuted_stat)

    p_value = np.sum(np.array(permuted_stats) >= observed_stat) / num_permutations

    return p_value


def bootstrap(
    group_a: np.ndarray,
    group_b: np.ndarray,
    func: Callable = np.mean,
    num_samples: int = 1000,
):
    bootstrap_differences = []

    for _ in range(num_samples):
        sample_a = np.random.choice(group_a, len(group_a), replace=True)
        sample_b = np.random.choice(group_b, len(group_b), replace=True)

        difference = func(sample_b) - func(sample_a)
        bootstrap_differences.append(difference)

    p_value = np.sum(np.array(bootstrap_differences) <= 0) / len(bootstrap_differences)

    return p_value, bootstrap_differences


def bootstrap_concat(
    group_a: np.ndarray,
    group_b: np.ndarray,
    func: Callable = np.mean,
    num_samples: int = 1000,
):
    concatenated_group = np.concatenate((group_a, group_b), axis=None)
    observed_stat = func(group_b) - func(group_a)

    bootstrap_stats = []

    for _ in range(0, num_samples):
        boot_sample = np.random.choice(
            concatenated_group, size=len(concatenated_group), replace=True
        )
        boot_sample_a = boot_sample[0 : len(group_a)]
        boot_sample_b = boot_sample[len(group_a) :]
        boot_stat = func(boot_sample_a) - func(boot_sample_b)
        bootstrap_stats.append(boot_stat)

    p_value = np.sum(np.array(bootstrap_stats) >= observed_stat) / len(bootstrap_stats)

    return p_value, bootstrap_stats


# def bootstrap_ci(
#     group_a: np.ndarray, group_b: np.ndarray, func: Callable = np.mean, num_samples: int = 1000
# ):
#     n = 1000
#     B = 10000
#     alpha = 0.05

#     pe = np.quantile(values_b, 0.9) - np.quantile(values_a, 0.9)
#     bootstrap_values_a = np.random.choice(values_a, (B, len(group_a)), True)
#     bootstrap_metrics_a = np.quantile(bootstrap_values_a, 0.9, axis=1)
#     bootstrap_values_b = np.random.choice(values_b, (B, len(group_a)), True)
#     bootstrap_metrics_b = np.quantile(bootstrap_values_b, 0.9, axis=1)
#     bootstrap_stats = bootstrap_metrics_b - bootstrap_metrics_a

#     ci = np.quantile(bootstrap_stats, [alpha / 2, 1 - alpha / 2])

#     return ci


def simulation_normal(
    test: Callable,
    alpha: int,
    beta: int,
    effect: float,
    mean_a: float,
    std_a: float,
    std_b: float = None,
    sample_size=5000,
    num_experiments: int = 100,
):
    if std_b == None:
        std_b = std_a

    change_detected = 0
    for _ in range(num_experiments):
        control_group = np.random.normal(size=sample_size, scale=2.1, loc=10)
        treatment_group = np.random.normal(size=sample_size, scale=2.1, loc=10.13)

        p_value = test(control_group, treatment_group)

        if p_value < alpha:
            change_detected += 1


def simulation_bernoulli():
    pass


# def find_percentiles(df: pd.DataFrame, percentile: float):
#     percentiles = df.apply(lambda col: np.percentile(col, percentile))

#     if ("conv" in df.columns) and ("revenue" in df.columns):
#         percentiles["revenue_nonzeros"] = np.percentile(df[df["conv"] != 0]["revenue"], percentile)

#     return percentiles

# def permutation_test(old: pd.DataFrame, new: pd.DataFrame, num_permutations=500):
#     df = pd.DataFrame(columns=["statistic", "p_value"], index=old.columns)

#     for col in old.columns:
#         test = stats.permutation_test(
#             (np.array(old[col], dtype=float), np.array(new[col], dtype=float)),
#             statistic=lambda x, y: x.mean() - y.mean(),
#             n_resamples=num_permutations,
#         )
#         df.loc[col] = test.statistic, test.pvalue
#         print(f"{col} done")

#     if ("conv" in old.columns) and ("revenue" in old.columns):
#         test = stats.permutation_test(
#             (
#                 np.array(old[old["conv"] != 0]["revenue"], dtype=float),
#                 np.array(new[new["conv"] != 0]["revenue"], dtype=float),
#             ),
#             statistic=lambda x, y: x.mean() - y.mean(),
#             n_resamples=num_permutations,
#         )
#         df.loc["revenue_nonzeros"] = test.statistic, test.pvalue
#         print(f"revenue_nonzeros done")

#     return df

# def t_test(old: pd.Series, new: pd.Series):
#     df = pd.DataFrame(columns=["statistic", "p_value"], index=old.columns)

#     for col in old.columns:
#         df.loc[col] = stats.ttest_ind(
#             np.array(old[col], dtype=float), np.array(new[col], dtype=float)
#         )

#     if ("conv" in old.columns) and ("revenue" in old.columns):
#         df.loc["revenue_nonzeros"] = stats.ttest_ind(
#             np.array(old[old["conv"] != 0]["revenue"], dtype=float),
#             np.array(new[new["conv"] != 0]["revenue"], dtype=float),
#         )

#     return df

# def reduce_uids(df: pd.DataFrame):
#     return df.groupby("uid").agg(max)

# if __name__ == "__main__":
#     import utils

#     import pandas as pd
#     import numpy as np
#     import seaborn as sns

#     df = utils.generate_df(ids_num=int(1e5))
#     old = df[df.group == 0].drop(columns=["uid", "group"])
#     new = df[df.group == 1].drop(columns=["uid", "group"])

#     print(permutation_test(old, new))