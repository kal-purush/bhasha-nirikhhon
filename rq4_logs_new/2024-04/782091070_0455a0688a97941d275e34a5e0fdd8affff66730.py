import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

url = 'https://raw.githubusercontent.com/modpanyakorn/prediction-data-using-linear-regression-model/main/Advertising.csv'
df = pd.read_csv(url)

st.set_page_config(page_title="Prediction", page_icon="favicon_32x32_XX0_icon.ico")

st.header("🤖 Sales Predictor from Ad Spending 💸")

st.sidebar.markdown("<h1 style='text-align: center;'>Predictor</h1>", unsafe_allow_html=True)
st.sidebar.subheader('', divider='rainbow')
x_column = st.sidebar.selectbox('เลือก X (Independent Variable)', df[['TV', 'Radio', 'Newspaper']].columns)
y_column = df[['Sales']].columns
st.sidebar.subheader('', divider='rainbow')
x_pre = st.sidebar.text_input(f'ใส่จำนวน "หน่วย" ของ {x_column} ที่ต้องการทำนาย Sales ','')
button = st.sidebar.button("Predict","primary")

st.info('👈👈ใส่จำนวน "หน่วย" ที่ต้องการทางซ้ายมือ')

fig2D = go.Figure()

if x_column == "TV":
    st.write(f"Scatter Plot ระหว่าง {x_column} กับ Sales")
    beta_0, beta_1 = 7.3249195736743005, 0.04644915342503764
    st.write(beta_0, beta_1)
    fig2D.add_trace(go.Scatter(x=df['TV'], y=df['Sales'], mode='markers', name='Data Point'))
    fig2D.add_trace(go.Scatter(x=df['TV'], y=beta_0 + beta_1 * df['TV'], mode='lines', name='Regression Line', line_color='red'))
    fig2D.update_layout(
        xaxis_title=x_column + " (X)",
        yaxis_title="Sales (y)"
    )
    st.plotly_chart(fig2D)
    if x_pre == "":
        st.error("กรุณาใส่จำนวนหน่วย")
    else:
        y = beta_0 + beta_1 * float(x_pre)
        st.success(f"ราคาที่คาดว่าจะได้จากการลงทุนโฆษณาจาก TV คือ: {round(y, 4)} ที่ R-squared: 0.6741")

elif x_column == "Radio":
    if x_pre == "":
        st.write()

    beta_0, beta_1 = 9.415011920008467, 0.2062025214221075
    fig2D.add_trace(go.Scatter(x=df['Radio'], y=df['Sales'], mode='markers', name='Data Point'))
    fig2D.add_trace(go.Scatter(x=df['Radio'], y=9.415011920008467 + 0.2062025214221075 * df['Radio'], mode='lines', name='Regression Line', line_color='red'))
    st.write(beta_0, beta_1)
    fig2D.update_layout(
        xaxis_title=x_column + " (X)",
        yaxis_title="Sales (y)"
    )
    st.plotly_chart(fig2D)
    y = beta_0 + beta_1 * float(x_pre)
    st.success(f"ราคาที่คาดว่าจะได้จากการลงทุนโฆษณาจาก Radio คือ: {round(y, 4)} ที่ R-squared: 0.4255")

elif x_column == "Newspaper":
    if x_pre == "":
        st.write()
        
    beta_0, beta_1 = 12.871689528888744, 0.04485626204696058
    fig2D.add_trace(go.Scatter(x=df['Newspaper'], y=df['Sales'], mode='markers', name='Data Point'))
    fig2D.add_trace(go.Scatter(x=df['Newspaper'], y=beta_0 + beta_1 * df['Newspaper'], mode='lines', name='Regression Line', line_color='red'))
    fig2D.update_layout(
        xaxis_title=x_column + " (X)",
        yaxis_title="Sales (y)"
    )
    st.plotly_chart(fig2D)
    y = beta_0 + beta_1 * float(x_pre)
    st.success(f"ราคาที่คาดว่าจะได้จากการลงทุนโฆษณาจาก Newspaper คือ: {round(y, 4)} ที่ R-squared: 0.0478")


st.write("Scatter Plot 3D")
fig3D = go.Figure(data=[go.Scatter3d(
    x=df['TV'],
    y=df['Radio'],
    z=df['Sales'],
    mode='markers',
    marker=dict(
        size=6,
        color=df['Sales'],                   
        colorscale='magma',             
        opacity=0.9
    )
)])

fig3D.update_layout(scene=dict(
                    xaxis_title='TV (X)',
                    yaxis_title='Radio (y)',
                    zaxis_title='Sales (z)'),
                  margin=dict(l=0, r=0, b=0, t=0),
                  xaxis=dict(gridcolor='rgba(255, 0, 0, 0.5)', 
                             gridwidth=10),  
                  yaxis=dict(gridcolor='red',  
                             gridwidth=1))  
st.plotly_chart(fig3D)