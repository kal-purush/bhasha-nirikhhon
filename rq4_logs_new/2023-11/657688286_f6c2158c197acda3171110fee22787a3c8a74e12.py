from pathlib import Path
import numpy as np
import json
import random

import opypy
import common


def _measure_accepted_rate(order: opypy.Orders,
                           threshold: float,
                           ndim: int,
                           beta: float,
                           scale: float,
                           seed: int,
                           w: np.ndarray):
    burn_in = 1000
    sampling = 1000

    K = np.array([[5 * (i != j) for j in range(ndim)]
                 for i in range(ndim)])
    mcmc = opypy.MCMC(w, K, threshold, beta, scale, seed, order=order)
    for _ in range(burn_in):
        mcmc.step()
    accepted_rate = np.mean(
        np.array([mcmc.step() == opypy.MCMCResult.Accepted for _ in range(sampling)]))
    return accepted_rate


def _measure_exchange_rate(order: opypy.Orders,
                           threshold: float,
                           ndim: int,
                           betas: list[float],
                           scales: list[float],
                           seed: int,
                           w: np.ndarray):
    burn_in = 1000
    num_exchange = 1000
    exchange_interval = 100

    K = np.array([[5 * (i != j) for j in range(ndim)]
                 for i in range(ndim)])

    M = len(betas)
    mcmc_list = opypy.RepricaMCMC(w, K, threshold, betas, scales, seed, order)
    stat = [[]] * (M-1)

    # Burn-In
    mcmc_list.step(burn_in)

    # Sampling
    for _ in range(num_exchange):
        mcmc_list.step(exchange_interval)

        exchange_res = mcmc_list.exchange()
        for (i, res) in filter(lambda x: x[1] is not None, enumerate(exchange_res)):
            stat[i].append(int(res))

    exchange_rate = [sum(series) / len(series) for series in stat]
    return exchange_rate


def experiment(datadir, order: opypy.Orders, threshold: float, ndim: int):
    identity = f"{str(order)}_{ndim}_{threshold:.4f}"
    datadir = datadir / identity
    if not datadir.exists():
        datadir.mkdir()

    target_accepted_rate = 0.5
    target_exchange_rate = 0.5
    min_beta = 1.0
    max_beta = 100.
    bisec_iteration = 10
    _seeder = random.Random(42)
    def seeder(): return _seeder.randint(0, 256)

    w = common.create_w(ndim)

    print(f"==== {identity}", flush=True)

    # min_betaに対するscaleを二分探索する
    _scale_l, _scale_r = 1e-10, 5
    for _ in range(bisec_iteration):
        print(f"{_scale_l:.6f} < - > {_scale_r:.6f}", flush=True)
        scale = (_scale_l + _scale_r) / 2

        accepted_rate = _measure_accepted_rate(
            order, threshold, ndim, min_beta, scale, seeder(), w)

        print(f"{accepted_rate:.4f}", flush=True)
        if target_accepted_rate < accepted_rate:
            _scale_l = scale
        else:
            _scale_r = scale

    # min_betaに対するscaleを使って、他のbetaに対してもscaleを決める仕組み
    def create_scaler(beta, scale): return lambda b: beta / b * scale
    scaler = create_scaler(min_beta, scale)

    # max_betaを超えるまで追加のbetaを二分探索で交換率を元に追加していく。
    betas = [min_beta]
    scales = [scaler(min_beta)]
    while betas[-1] < max_beta:
        _beta_l, _beta_r = betas[-1], max_beta * 2
        for _ in range(bisec_iteration):
            print(f"{_beta_l:.6f} < - > {_beta_r:.6f}", flush=True)
            beta = (_beta_l + _beta_r) / 2

            exchange_rates = _measure_exchange_rate(
                order, threshold, ndim,
                betas + [beta],
                scales + [scaler(beta)],
                seeder(), w)

            print(exchange_rates, flush=True)
            if target_exchange_rate < exchange_rates[-1]:
                _beta_l = beta
            else:
                _beta_r = beta

        betas.append(beta)
        scales.append(scaler(beta))

    result = {
        "betas": betas,
        "scales": scales,
        "condition": {
            "order": str(order),
            "ndim": ndim,
            "threshold": threshold,
            "w": w.tolist(),
            "min_beta": min_beta,
            "max_beta": max_beta,
            "target_accepted_rate": target_accepted_rate,
            "target_exchange_rate": target_exchange_rate,
        }
    }
    with open(datadir / "result.json", "w") as f:
        json.dump(result, f, indent=4)


def main():
    file = Path(__file__)
    data = file.parent / "output" / file.stem
    if not data.exists():
        data.mkdir()

    experiment(data, "kuramoto", 0.8, 2)
    experiment(data, "num_of_avg_freq_mode", 0.9, 2)

    experiment(data, "kuramoto", 0.8,  3)
    experiment(data, "num_of_avg_freq_mode", 0.9,  3)


if __name__ == "__main__":
    main()