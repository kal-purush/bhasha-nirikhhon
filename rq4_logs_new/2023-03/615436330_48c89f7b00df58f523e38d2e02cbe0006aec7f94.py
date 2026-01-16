import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Tabela Premium Colchoes",
    layout="wide"

)

st.title('TABELA DE PREÇOS PREMIUM COLCHOES \n:sunglasses: :moneybag:')


def formatar(valor):
    return "{:,.f}".format(valor)

df = pd.read_excel(
    io='tabela_premium.xlsx',
    engine='openpyxl',
    usecols=range(0, 3)
    
)

codigo=st.sidebar.multiselect(
  "SELECIONE OS CODIGOS:",
   options=df["CODIGOS"].unique(),
   #default=df["CODIGOS"].unique()  
  )

# produto=st.sidebar.multiselect(
#   "SELECIONE OS PRODUTOS:",
#    options=df["PRODUTOS"].unique(),
#    #default=df["CODIGOS"].unique()  
#   )

df_select = df.query(
    "CODIGOS == @codigo",
)

total_produtos = df_select["VALORES"].sum()
texto_soma_total_produtos = f"{total_produtos: _.2f}"
texto_soma_total_produtos = texto_soma_total_produtos.replace('.', ',').replace('_', '.')
left_column, middle_column, right_column = st.columns(3)
with right_column:
    st.write(f"TOTAL DA SELEÇÃO R$ {texto_soma_total_produtos}")


st.table(df_select)
