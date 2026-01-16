import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import plotly.express as px

# @st.cache_data
df = pd.read_csv('cleaned_fraud_data.csv')



def show_explore_page():
    st.title("Fraud Report Submissions Data Analysis Overview")
    st.write(""" ### 2021-2024 Canadian Anti-Fraud Centre Fraud Reporting Data """)

    data = df["Victim Result"].value_counts()
    st.write("""#### Overall Victims of Fraud Count""")
    st.bar_chart(data.rename(index={0: 'No Fraud', 1: 'Victim of Fraud'}))


    data = df["Major Province/State"].value_counts()

    fig1, ax1 = plt.subplots()
    ax1.barh(width=data, y=data.index)
    plt.xticks(rotation=45)
    st.write("""#### Total Report Count per Major Province or State""")
    st.pyplot(fig1)


    import plotly.graph_objects as go

    data = (
    df.groupby(['Major Province/State', 'Victim Result'])
    .size()
    .reset_index(name='Count')
    )

    total_counts = data.groupby("Victim Result")["Count"].transform('sum')
    data['Percentage'] = (data['Count'] / total_counts) * 100

    age_groups = data["Major Province/State"].unique()
    victim_results = data["Victim Result"].unique()

    traces = []
    colors = ['#4C72B0', '#C44E52', '#55A868', '#8172B2']  #change these however you want
    for i, victim_result in enumerate(victim_results):
        group_data = data[data["Victim Result"] == victim_result]
        traces.append(
        go.Bar(
            x=group_data["Major Province/State"],
            y=group_data['Count'],
            name=f"Victim Result: {victim_result}",
            marker_color=colors[i % len(colors)], 
            text=group_data['Percentage'].apply(lambda x:
            f'{x:.1f}%'),
            textposition='auto',  
        )
        )
    fig = go.Figure(data=traces)

    fig.update_layout(
        title='Fraud Victim Result by Major Province/State',
        xaxis_title='Major Province/State',
        yaxis_title='Count',
        xaxis_tickangle=-45, 
        barmode='group', 
    )

    st.plotly_chart(fig)

    data = df["Victim Age Group"].value_counts()

    fig1, ax1 = plt.subplots()
    ax1.barh(width=data, y=data.index)
    plt.xticks(rotation=45)
    st.write("""#### Total Report Count per Age Group""")
    st.pyplot(fig1)



    import plotly.graph_objects as go

    data = (
    df.groupby(['Victim Age Group', 'Victim Result'])
    .size()
    .reset_index(name='Count')
    )

    total_counts = data.groupby("Victim Result")["Count"].transform('sum')
    data['Percentage'] = (data['Count'] / total_counts) * 100

    age_groups = data["Victim Age Group"].unique()
    victim_results = data["Victim Result"].unique()

    traces = []
    colors = ['#4C72B0', '#C44E52', '#55A868', '#8172B2']  #change these however you want
    for i, victim_result in enumerate(victim_results):
        group_data = data[data["Victim Result"] == victim_result]
        traces.append(
        go.Bar(
            x=group_data["Victim Age Group"],
            y=group_data['Count'],
            name=f"Victim Result: {victim_result}",
            marker_color=colors[i % len(colors)], 
            text=group_data['Percentage'].apply(lambda x:
            f'{x:.1f}%'),
            textposition='auto',  
        )
        )
    fig = go.Figure(data=traces)

    fig.update_layout(
        title='Fraud Victim Result by Age Group',
        xaxis_title='Age Group',
        yaxis_title='Count',
         
        barmode='group', 
    )

    st.plotly_chart(fig)




    import plotly.graph_objects as go

    data = (
    df.groupby(['Gender', 'Victim Result'])
    .size()
    .reset_index(name='Count')
    )

    total_counts = data.groupby("Victim Result")["Count"].transform('sum')
    data['Percentage'] = (data['Count'] / total_counts) * 100

    age_groups = data["Gender"].unique()
    victim_results = data["Victim Result"].unique()

    traces = []
    colors = ['#4C72B0', '#C44E52', '#55A868', '#8172B2']  #change these however you want
    for i, victim_result in enumerate(victim_results):
        group_data = data[data["Victim Result"] == victim_result]
        traces.append(
        go.Bar(
            x=group_data["Gender"],
            y=group_data['Count'],
            name=f"Victim Result: {victim_result}",
            marker_color=colors[i % len(colors)], 
            text=group_data['Percentage'].apply(lambda x:
            f'{x:.1f}%'),
            textposition='auto',  
        )
        )
    fig = go.Figure(data=traces)

    fig.update_layout(
        title='Fraud Victim Result by Gender',
        xaxis_title='Gender',
        yaxis_title='Count',
        
        barmode='group', 
    )

    st.plotly_chart(fig)




    data = df.groupby(['Fraud/Cybercrime Category', 'Victim Result']).size().reset_index(name='Count')

    fig = px.bar(
    data,
    x='Fraud/Cybercrime Category',
    y='Count',
    color='Victim Result',  
    barmode='group',  
    title="Fraud Victim Result by Fraud/Cybercrime Category",
    labels={'Count': 'Number of Victims', 'Fraud/Cybercrime Category': 'Fraud/Cybercrime Category'},
    )

    fig.update_layout(
    xaxis=dict(tickangle=45) 
    )

    st.plotly_chart(fig)







    import plotly.graph_objects as go

    data = (
    df.groupby(['Solicitation Method', 'Victim Result'])
    .size()
    .reset_index(name='Count')
    )

    total_counts = data.groupby("Victim Result")["Count"].transform('sum')
    data['Percentage'] = (data['Count'] / total_counts) * 100

    age_groups = data["Solicitation Method"].unique()
    victim_results = data["Victim Result"].unique()

    traces = []
    colors = ['#4C72B0', '#C44E52', '#55A868', '#8172B2']  #change these however you want
    for i, victim_result in enumerate(victim_results):
        group_data = data[data["Victim Result"] == victim_result]
        traces.append(
        go.Bar(
            x=group_data["Solicitation Method"],
            y=group_data['Count'],
            name=f"Victim Result: {victim_result}",
            marker_color=colors[i % len(colors)], 
            text=group_data['Percentage'].apply(lambda x:
            f'{x:.1f}%'),
            textposition='auto',  
        )
        )
    fig = go.Figure(data=traces)

    fig.update_layout(
        title='Fraud Victim Result by Solicitation Method',
        xaxis_title='Solicitation Method',
        yaxis_title='Count',
        xaxis_tickangle=-45, 
        barmode='group', 
    )

    st.plotly_chart(fig)