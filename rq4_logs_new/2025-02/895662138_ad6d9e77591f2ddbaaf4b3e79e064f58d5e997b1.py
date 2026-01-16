# plotting functions used in the dogs_in_zurich app

import plotly.express as px


def plot_linegraphs_zurich(df, x_values, y_values, color=None):

    fig_ = px.line(df,
              x=x_values,
              y=y_values,
              color=color,
              width=800, height=600)

    # Highlight 2020-2022
    fig_.add_vrect(
        x0=2020, x1=2022,
        fillcolor="green",
        opacity=0.5,
        line_width=0)

    # Add annotation for COVID-19
    fig_.add_annotation(
        x=2021.2, y=max(df[y_values]),
        text="COVID-19 Pandemic",
        showarrow=False,
        font=dict(size=12, color="white"),
        xanchor="center", yanchor="bottom")

    # Ensure every year is shown on the x-axis
    all_years = sorted(df[x_values])
    fig_.update_xaxes(
        tickmode="array",      # Force specific tick values
        tickvals=all_years,    # List of years
        )

    return fig_