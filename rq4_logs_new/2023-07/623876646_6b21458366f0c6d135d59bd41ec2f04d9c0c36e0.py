import jax.numpy as jnp
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from lcm.entry_point import get_lcm_function


def compute_analytical_consumption(consumption_function, grid, work_status):
    """Compute consumption values for a given grid.

    Args:
        consumption_function (callable): Consumption function.
        grid (np.ndarray): Grid of wealth values.
        work_status (bool): Whether the agent is working or not.

    Returns:
        list: Consumption values for the given grid.

    """
    return [consumption_function(wealth=w, work_status=work_status) for w in grid]


def compute_numerical_consumption(
    model,
    model_solution,
    model_params,
    period,
    lagged_retirement,
):
    """Simulate consumption values given numerical model solution.

    Args:
        model (dict): Model specification.
        model_solution (list): Numerical model solution.
        model_params (dict): Model parameters.
        period (int): Period for which to compute consumption.
        lagged_retirement (float, optional): Lagged retirement status.

    Returns:
        list: Consumption values for the given grid.

    """
    grid_size = model["states"]["wealth"]["n_points"]
    simulate_model, _ = get_lcm_function(model=model, targets="simulate")
    simulation_results = simulate_model(
        model_params,
        vf_arr_list=model_solution[period:],
        initial_states={
            "wealth": jnp.linspace(
                start=model["states"]["wealth"]["start"],
                stop=model["states"]["wealth"]["stop"],
                num=grid_size,
            ),
            "lagged_retirement": jnp.repeat(lagged_retirement, repeats=grid_size),
        },
    )
    return simulation_results[0]["choices"]["consumption"]


def prepare_plotting_data(
    analytical_data,
    numerical_data,
    grid,
    n_periods,
):
    """Prepare data for plotting.

    Args:
        analytical_data (list): Analytical consumption values.
        numerical_data (numpy.ndarray): Numerical consumption values.
        grid (list): Grid of wealth values.
        n_periods (int): Number of periods.

    Returns:
        pd.DataFrame: Data for plotting.

    """
    analytical = pd.DataFrame(
        np.array(analytical_data).T,
        columns=[f"value{i}" for i in range(n_periods)],
    )
    numerical = pd.DataFrame(
        numerical_data.T,
        columns=[f"value{i}" for i in range(n_periods)],
    )
    plot_data = pd.concat(
        [analytical, numerical],
        axis=0,
        keys=["analytical", "numerical"],
        names=["calculation_procedure", "wealth_index"],
    ).reset_index()
    plot_data = (
        pd.wide_to_long(
            plot_data,
            stubnames="value",
            i=["calculation_procedure", "wealth_index"],
            j="period",
        )
        .reset_index()
        .set_index(["calculation_procedure", "period", "wealth_index"])
        .sort_index()
    )
    plot_data["wealth"] = np.tile(grid, 2 * n_periods)
    plot_data = plot_data.reset_index()

    return plot_data


def plot_consumption_function(
    model,
    analytical_consumption_fct=None,
    lcm_solution=None,
    work_status=True,
    model_params=None,
    period=0,
):
    """Plot the consumption function for a given period.

    Plots consumption function depending on wealth for a given period.

    Args:
        model (dict): Model specification.
        analytical_consumption_fct (callable, optional): Analytical consumption
            function. Defaults to None.
        lcm_solution (list, optional): Numerical model solution.
            Defaults to None.
        work_status (bool, optional): Whether the agent is working or not.
            Defaults to True.
        model_params (dict, optional): Model parameters. Defaults to None.
        period (int, optional): Period for which to compute consumption. Defaults to 0.

    Returns:
        go.Figure: Plotly figure.

    """
    grid_start = model["states"]["wealth"]["start"]
    grid_stop = model["states"]["wealth"]["stop"]
    grid_size = model["states"]["wealth"]["n_points"]
    grid = np.linspace(grid_start, grid_stop, grid_size)

    fig = go.Figure()
    if analytical_consumption_fct is not None:
        analytical_consumption = compute_analytical_consumption(
            consumption_function=analytical_consumption_fct[period],
            work_status=work_status,
            grid=grid,
        )
        fig.add_trace(
            go.Scatter(
                x=grid,
                y=analytical_consumption,
                mode="markers",
                name="Analytical consumption function",
            ),
        )
    if lcm_solution is not None:
        numerical_consumption = compute_numerical_consumption(
            model=model,
            model_solution=lcm_solution,
            model_params=model_params,
            period=period,
            lagged_retirement=1 - work_status,
        )
        fig.add_trace(
            go.Scatter(
                x=grid,
                y=numerical_consumption,
                mode="markers",
                name="Simulated consumption function",
            ),
        )
    fig.update_layout(
        title=f"Consumption function for period {period}",
        xaxis_title="Wealth",
        yaxis_title="Consumption",
    )
    return fig


def plot_consumption_profiles(
    analytical_consumption,
    numerical_consumption,
    grid,
):
    """Plot consumption profiles for a given grid.

    Args:
        analytical_consumption (list): Analytical consumption values.
        numerical_consumption (list): Numerical consumption values.
        grid (list): Grid of wealth values.

    """
    # Prepare Data for Plotting
    n_periods = len(analytical_consumption)
    plot_data = prepare_plotting_data(
        analytical_data=analytical_consumption,
        numerical_data=numerical_consumption,
        grid=grid,
        n_periods=n_periods,
    )

    # Plot Data
    fig = px.scatter(
        plot_data.reset_index(),
        x="period",
        y="value",
        color="calculation_procedure",
        animation_frame="wealth",
    )
    fig.update_layout(
        title="Consumption profiles",
        xaxis_title="Period",
        yaxis_title="Consumption",
        xaxis={
            "range": [-0.5, plot_data["period"].max() + 1],
        },
        yaxis={
            "range": [-1, plot_data["value"].max() + 1],
        },
    )
    return fig


def plot_value_functions(analytical_solution, numerical_solution, grid):
    """Plot value functions.

    Args:
        analytical_solution (dict): Analytical value array.
        numerical_solution (dict): Numerical value array.
        grid (list): Grid of wealth values.

    Returns:
        go.Figure: Plotly figure.

    """
    # Prepare Data for Plotting
    figs = {}
    n_periods = len(analytical_solution["worker"])
    agent_states = ["worker", "retired"]
    for state in agent_states:
        plot_data = prepare_plotting_data(
            analytical_data=analytical_solution[state],
            numerical_data=numerical_solution[state],
            grid=grid,
            n_periods=n_periods,
        )
        figure = px.line(
            plot_data,
            x="wealth",
            y="value",
            color="calculation_procedure",
            animation_frame="period",
        )
        figure.update_layout(
            title=f"Value function {state.capitalize()}",
            xaxis_title="Wealth",
            yaxis_title="Value",
            xaxis={
                "range": [-1, plot_data["wealth"].max() + 1],
            },
            yaxis={
                "range": [-1, plot_data["value"].max() + 1],
            },
        )

        figs[state] = figure

    return figs