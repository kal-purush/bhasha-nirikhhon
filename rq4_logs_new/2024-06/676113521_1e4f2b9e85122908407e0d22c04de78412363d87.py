import argparse
import cProfile  # noqa
import os
import pstats  # noqa
import time

import jax
import numpy as np
import pandas as pd
import wandb  # noqa
import yaml

from nqs.state import nqs

jax.config.update("jax_platform_name", "cpu")

# Print device
print(jax.devices())


def load_config(config_path):
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def initialize_system(config):
    system = nqs.NQS(
        nqs_repr="psi",
        backend=config["backend"],
        logger_level="INFO",
        seed=config["seed"],
    )

    if config["nqs_type"] == "ffnn":
        base_layer_sizes = config["base_layer_sizes"][config["nqs_type"]]
        layer_sizes = [config["nparticles"] * config["dim"]] + base_layer_sizes
        activations = config["activations"].get(config["nqs_type"], [])
        common_kwargs = {
            "layer_sizes": layer_sizes,
            "activations": activations,
            "correlation": config["correlation"],
            "particle": config["particle"],
        }
        system.set_wf(
            config["nqs_type"], config["nparticles"], config["dim"], **common_kwargs
        )
    elif config["nqs_type"] == "dsffn":
        common_kwargs = {
            "layer_sizes": {
                "S0": [config["dim"]]
                + config["base_layer_sizes"][config["nqs_type"]]["S0"]
                + [config["latent_dim"]],
                "S1": [config["latent_dim"]]
                + config["base_layer_sizes"][config["nqs_type"]]["S1"],
            },
            "activations": {
                "S0": config["activations"].get(config["nqs_type"], [])["S0"],
                "S1": config["activations"].get(config["nqs_type"], [])["S1"],
            },
            "correlation": config["correlation"],
            "particle": config["particle"],
        }
        print("layer_sizes", common_kwargs["layer_sizes"])
        print("activations", common_kwargs["activations"])
        system.set_wf(
            config["nqs_type"], config["nparticles"], config["dim"], **common_kwargs
        )
    elif config["nqs_type"] == "rbm":
        system.set_wf(
            config["nqs_type"],
            config["nparticles"],
            config["dim"],
            nhidden=config["nhidden"],
            sigma2=1.0 / np.sqrt(config["omega"]),
            particle=config["particle"],
            correlation=config["correlation"],
        )
    else:
        # Handle other methods like VMC that don't use layers
        system.set_wf(
            config["nqs_type"],
            config["nparticles"],
            config["dim"],
            particle=config["particle"],
            correlation=config["correlation"],
        )

    return system


def run_experiment(config):
    system = initialize_system(config)
    mcmc_alg = config["mcmc_alg"]
    system.set_sampler(
        mcmc_alg=mcmc_alg,
        scale=(
            0.5 / np.sqrt(config["nparticles"] * config["dim"])
            if mcmc_alg == "m"
            else 0.1 / np.sqrt(config["nparticles"])
        ),
    )
    system.set_hamiltonian(
        type_="ho",
        int_type=config["interaction_type"],
        omega=config["omega"],  # needs to be fixed to 1 to compare to drissi et al
        r0_reg=config["r0_reg"],
        training_cycles=config["training_cycles"],
    )
    system.set_optimizer(
        optimizer=config["optimizer"],
        eta=config["eta"] / np.sqrt(config["nparticles"] * config["dim"]),
    )
    common_kwargs = {}
    if config["nqs_type"] == "ffnn":
        common_kwargs = {
            "layer_sizes": [config["nparticles"] * config["dim"]]
            + config["base_layer_sizes"][config["nqs_type"]],
            "activations": config["activations"].get(config["nqs_type"], []),
            "correlation": config["correlation"],
            "particle": config["particle"],
        }

    if config["nqs_type"] == "dsffn":
        common_kwargs = {
            "layer_sizes": {
                "S0": [config["dim"]]
                + config["base_layer_sizes"][config["nqs_type"]]["S0"]
                + [config["latent_dim"]],
                "S1": [config["latent_dim"]]
                + config["base_layer_sizes"][config["nqs_type"]]["S1"],
            },
            "activations": {
                "S0": config["activations"].get(config["nqs_type"], [])["S0"],
                "S1": config["activations"].get(config["nqs_type"], [])["S1"],
            },
            "correlation": config["correlation"],
            "particle": config["particle"],
        }
    if config["pretrain"]:
        system = pretrain(
            system,
            max_iter=1000,
            batch_size=500,
            args=common_kwargs,
        )
    time_train = time.time()
    history = system.train(  # noqa
        max_iter=config["training_cycles"],
        batch_size=config["batch_size"],
        early_stop=False,
        history=True,
        tune=False,
        grad_clip=0,
        seed=config["seed"],
    )
    time_train = time.time() - time_train

    # save energies and stds to csv, together
    df_hist = pd.DataFrame(  # noqa
        {
            "energy": history["energy"],
            "std_error": history["std"],
            "nqs_type": config["nqs_type"],
            "n_particles": config["nparticles"],
            "dim": config["dim"],
            "batch_size": config["batch_size"],
            "eta": config["eta"],
            "training_cycles": config["training_cycles"],
            "nsamples": config["nsamples"],
            "Opti": config["optimizer"],
            "particle": config["particle"],
        }
    )
    # if not os.path.exists(
    #     config["output_filename"] + f"energies_and_stds_v0_{config['omega']}.csv"
    # ):
    #     df_hist.to_csv(
    #         config["output_filename"] + f"energies_and_stds_v0_{config['omega']}.csv",
    #         index=False,
    #     )
    # else:
    #     df_hist.to_csv(
    #         config["output_filename"] + f"energies_and_stds_v0_{config['omega']}.csv",
    #         mode="a",
    #         header=False,
    #         index=False,
    #     )

    time_sample = time.time()
    df_all = system.sample(
        config["nsamples"],
        config["nchains"],
        config["seed"],
        save_positions=config["save_positions"],
        foldername=config["output_filename"],
    )
    time_sample = time.time() - time_sample

    # Mean values
    accept_rate_mean = df_all["accept_rate"].mean()

    # Combined standard error of the mean for energy
    E_means = df_all["E_energy"]
    E_std_errors = df_all["E_std_error"]
    E_variances = E_std_errors**2
    E_weights = 1 / E_variances
    E_combined_mean = np.sum(E_weights * E_means) / np.sum(E_weights)
    E_combined_variance = 1 / np.sum(E_weights)
    E_combined_std_error = np.sqrt(E_combined_variance)

    # Combined standard error of the mean for kinetic energy
    K_means = df_all["K_energy"]
    K_std_errors = df_all["K_std_error"]
    K_variances = K_std_errors**2
    K_weights = 1 / K_variances
    K_combined_mean = np.sum(K_weights * K_means) / np.sum(K_weights)
    K_combined_variance = 1 / np.sum(K_weights)
    K_combined_std_error = np.sqrt(K_combined_variance)

    # Combined standard error of the mean for potential energy (trap)
    PE_trap_means = df_all["PE_trap_energy"]
    PE_trap_std_errors = df_all["PE_trap_std_error"]
    PE_trap_variances = PE_trap_std_errors**2
    PE_trap_weights = 1 / PE_trap_variances
    PE_trap_combined_mean = np.sum(PE_trap_weights * PE_trap_means) / np.sum(
        PE_trap_weights
    )
    PE_trap_combined_variance = 1 / np.sum(PE_trap_weights)
    PE_trap_combined_std_error = np.sqrt(PE_trap_combined_variance)

    # Combined standard error of the mean for potential energy (int)
    PE_int_means = df_all["PE_int_energy"]
    PE_int_std_errors = df_all["PE_int_std_error"]
    PE_int_variances = PE_int_std_errors**2
    PE_int_weights = 1 / PE_int_variances
    PE_int_combined_mean = np.sum(PE_int_weights * PE_int_means) / np.sum(
        PE_int_weights
    )
    PE_int_combined_variance = 1 / np.sum(PE_int_weights)
    PE_int_combined_std_error = np.sqrt(PE_int_combined_variance)

    # Construct the combined DataFrame
    combined_data = {
        "E_energy": [E_combined_mean],
        "E_std_error": [E_combined_std_error],
        "E_variance": [np.mean(df_all["E_variance"])],
        "K_energy": [K_combined_mean],
        "K_std_error": [K_combined_std_error],
        "PE_trap_energy": [PE_trap_combined_mean],
        "PE_trap_std_error": [PE_trap_combined_std_error],
        "PE_int_energy": [PE_int_combined_mean],
        "PE_int_std_error": [PE_int_combined_std_error],
        "accept_rate": [accept_rate_mean],
        "omega": [config["omega"]],
        "pretrain": [config["pretrain"]],
        "r0_reg": [config["r0_reg"]],
        "correration": [config["correlation"]],
        "optimizer": [config["optimizer"]],
    }

    df_mean = pd.DataFrame(combined_data)

    final_E_energy = df_mean["E_energy"].values[0]
    final_K_energy = df_mean["K_energy"].values[0]
    final_PE_trap_energy = df_mean["PE_trap_energy"].values[0]
    final_PE_int_energy = df_mean["PE_int_energy"].values[0]
    final_E_error = df_mean["E_std_error"].values[0]
    final_K_error = df_mean["K_std_error"].values[0]
    final_PE_trap_error = df_mean["PE_trap_std_error"].values[0]
    final_PE_int_error = df_mean["PE_int_std_error"].values[0]
    # convert any NaN to 0
    final_E_error = 0 if np.isnan(final_E_error) else final_E_error
    final_K_error = 0 if np.isnan(final_K_error) else final_K_error
    final_PE_trap_error = 0 if np.isnan(final_PE_trap_error) else final_PE_trap_error
    final_PE_int_error = 0 if np.isnan(final_PE_int_error) else final_PE_int_error
    final_E_energy = 0 if np.isnan(final_E_energy) else final_E_energy
    final_K_energy = 0 if np.isnan(final_K_energy) else final_K_energy
    final_PE_trap_energy = 0 if np.isnan(final_PE_trap_energy) else final_PE_trap_energy
    final_PE_int_energy = 0 if np.isnan(final_PE_int_energy) else final_PE_int_energy

    E_error_str = f"{final_E_error:.0e}"
    K_error_str = f"{final_K_error:.0e}"
    PE_trap_error_str = f"{final_PE_trap_error:.0e}"
    PE_int_error_str = f"{final_PE_int_error:.0e}"

    E_error_scale = int(E_error_str.split("e")[-1])
    K_error_scale = int(K_error_str.split("e")[-1])
    PE_trap_error_scale = int(PE_trap_error_str.split("e")[-1])
    PE_int_error_scale = int(PE_int_error_str.split("e")[-1])

    E_energy_decimal_places = -E_error_scale
    K_energy_decimal_places = -K_error_scale
    PE_trap_energy_decimal_places = -PE_trap_error_scale
    PE_int_energy_decimal_places = -PE_int_error_scale

    # Format energy to match the precision required by the error
    if E_energy_decimal_places > 0:
        E_energy_str = f"{final_E_energy:.{E_energy_decimal_places}f}"
    else:
        E_energy_str = f"{int(final_E_energy)}"

    if K_energy_decimal_places > 0:
        K_energy_str = f"{final_K_energy:.{K_energy_decimal_places}f}"
    else:
        K_energy_str = f"{int(final_K_energy)}"

    if PE_trap_energy_decimal_places > 0:
        PE_trap_energy_str = f"{final_PE_trap_energy:.{PE_trap_energy_decimal_places}f}"
    else:
        PE_trap_energy_str = f"{int(final_PE_trap_energy)}"

    if PE_int_energy_decimal_places > 0:
        PE_int_energy_str = f"{final_PE_int_energy:.{PE_int_energy_decimal_places}f}"
    else:
        PE_int_energy_str = f"{int(final_PE_int_energy)}"

    # Get the first digit of the error for the parenthesis notation
    E_error_first_digit = E_error_str[0]
    K_error_first_digit = K_error_str[0]
    PE_trap_error_first_digit = PE_trap_error_str[0]
    PE_int_error_first_digit = PE_int_error_str[0]

    # Remove trailing decimal point if it exists after formatting
    if E_energy_str[-1] == ".":
        E_energy_str = E_energy_str[:-1]
    if K_energy_str[-1] == ".":
        K_energy_str = K_energy_str[:-1]
    if PE_trap_energy_str[-1] == ".":
        PE_trap_energy_str = PE_trap_energy_str[:-1]
    if PE_int_energy_str[-1] == ".":
        PE_int_energy_str = PE_int_energy_str[:-1]

    formated_E_energy = f"{E_energy_str}({E_error_first_digit})"
    formated_K_energy = f"{K_energy_str}({K_error_first_digit})"
    formated_PE_trap_energy = f"{PE_trap_energy_str}({PE_trap_error_first_digit})"
    formated_PE_int_energy = f"{PE_int_energy_str}({PE_int_error_first_digit})"

    df_mean["E_energy(error)"] = formated_E_energy
    df_mean["K_energy(error)"] = formated_K_energy
    df_mean["PE_trap_energy(error)"] = formated_PE_trap_energy
    df_mean["PE_int_energy(error)"] = formated_PE_int_energy

    df_mean[
        [
            "nqs_type",
            "n_particles",
            "dim",
            "batch_size",
            "eta",
            "training_cycles",
            "nsamples",
            "Opti",
            "particle",
            "time_train",
            "time_sample",
        ]
    ] = [
        config["nqs_type"],
        config["nparticles"],
        config["dim"],
        config["batch_size"],
        config["eta"] / np.sqrt(config["nparticles"] * config["dim"]),
        config["training_cycles"],
        config["nchains"] * config["nsamples"],
        config["optimizer"],
        config["particle"],
        time_train,
        time_sample,
    ]
    if not os.path.exists(
        config["output_filename"] + f"final_results_v0_{config['omega']}.csv"
    ):
        df_mean.to_csv(
            config["output_filename"] + f"final_results_v0_{config['omega']}.csv",
            index=False,
        )
    else:
        df_mean.to_csv(
            config["output_filename"] + f"final_results_v0_{config['omega']}.csv",
            mode="a",
            header=False,
            index=False,
        )

    print(df_mean)

    # if config["save_positions"]:
    #     chain_id = 0
    #     filename = f"energies_and_pos_{config['nqs_type'].upper()}_ch{chain_id}.h5"
    #     plot_3dobd(filename, config["nsamples"], config["dim"])

    # sns.scatterplot(data=df_all, x="chain_id", y="energy")
    # plt.xlabel("Chain")
    # plt.ylabel("Energy")
    # plt.show()


def pretrain(system, max_iter, batch_size, args):
    print("Pretraining with GAUSSIAN model first")
    if system.nqs_type == "ffnn" or system.nqs_type == "dsffn":
        system.pretrain(
            model="Gaussian",
            max_iter=max_iter,
            batch_size=batch_size,
            logger_level="INFO",
            args=args,
        )

    if config["interaction_type"].lower() != "none":
        print("Pretraining with non-interact FIRST")
        system.set_hamiltonian(
            type_="ho",
            int_type="none",
            omega=config["omega"],  # will be fixed to 1 to compare to drissi et al
            training_cycles=config["training_cycles"],
        )
        system.train(  # noqa
            max_iter=config["training_cycles"],
            batch_size=config["batch_size"],
            early_stop=False,
            history=True,
            tune=False,
            grad_clip=0,
            seed=config["seed"],
        )
        system.set_hamiltonian(
            type_="ho",
            int_type=config["interaction_type"],
            omega=config["omega"],  # needs to be fixed to 1 to compare to drissi et al
            r0_reg=config["reg_r0"],
            training_cycles=config["training_cycles"],
        )
    return system


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run NQS Experiment")
    parser.add_argument(
        "--config", type=str, required=True, help="Path to the configuration file"
    )
    args = parser.parse_args()

    # Ensure the config path is relative to the project root
    config_path = os.path.join(os.path.dirname(__file__), args.config)

    config = load_config(config_path)

    print(
        f"Running experiment with interaction: {config['interaction_type']} and n_particles: {config['nparticles']}"
    )

    # try:
    run_experiment(config)
    # except Exception as e:
    #    print(
    #        f"Error: {e} for interaction: {config['interaction_type']} and n_particles: {config['nparticles']}"
    #    )