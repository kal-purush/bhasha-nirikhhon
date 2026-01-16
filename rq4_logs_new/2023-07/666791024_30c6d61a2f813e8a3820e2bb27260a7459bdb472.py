from glob import glob

import pandas as pd
import powerlaw
import streamlit as st
import plotly.express as px
from streamlit_extras.dataframe_explorer import dataframe_explorer

st.set_page_config(layout='wide')
st.sidebar.title('PowerLaw')
st.sidebar.caption('Options')
add_filter = st.sidebar.checkbox('Add filter')
left, right = st.columns(2)


def preprocess(df, column, groupby='', top_n=-1):
    if groupby == '':
        df[groupby] = column
    df = df[df[column] > 0][[groupby, column]]
    if top_n > 0:
        data = df.groupby(groupby)[column].nlargest(top_n).droplevel(1).reset_index()
    else:
        data = df.sort_values(by=[groupby, column], ascending=[True, False])
    data['Rank'] = data.groupby(groupby).cumcount() + 1
    data[groupby] = data[groupby].astype(str)
    return data


with left:
    csv_paths = [''] + sorted(glob('data/**/*.csv', recursive=True))
    csv_path = st.selectbox('File', csv_paths, format_func=lambda x: x[5:-4])
    if csv_path == '':
        st.stop()
    df = pd.read_csv(csv_path)
    if add_filter:
        df = dataframe_explorer(df)
    st.dataframe(df, use_container_width=True)

    column = st.selectbox('Column', [''] + list(df.columns))
    if column == '':
        st.stop()
    groupby = st.selectbox('Group by', [''] + list(df.columns))

    with st.expander('Options', expanded=True):
        top_n = st.number_input('Top N', value=10000)

with right:
    data = preprocess(df, column, groupby, top_n)

    kwargs = dict(data_frame=data, x=column, y='Rank', color=groupby, log_x=True, log_y=True, opacity=0.5)
    fig = px.scatter(**kwargs)

    data['Rank'] = data['Rank'] - 0.5
    kwargs.update(dict(trendline='ols', trendline_options=dict(log_x=True, log_y=True)))
    ols_fig = px.scatter(**kwargs)
    for trace in ols_fig.data:
        if trace.mode == 'lines':
            trace.line.dash = 'dot'
            fig.add_trace(trace)

    st.plotly_chart(fig)

    ols_results = px.get_trendline_results(ols_fig)
    if groupby == '':
        ols_results[groupby] = column
    alpha_gab = ols_results.set_index(groupby)['px_fit_results'].apply(
        lambda x: pd.Series([-x.params[1], x.bse[1], 'gab'])).reset_index()
    alpha_pl = data.groupby(groupby).apply(
        lambda x: pd.Series(
            [powerlaw.Fit(x[column], xmin=x[column].min(), discrete=True).alpha, 0, 'pl'])).reset_index()
    alpha_df = pd.concat([alpha_gab, alpha_pl]).rename(columns={0: 'alpha', 1: 'error', 2: 'model'})
    fig = px.scatter(alpha_df, x=groupby, y='alpha', error_y='error', color='model')
    st.plotly_chart(fig, use_container_width=True)