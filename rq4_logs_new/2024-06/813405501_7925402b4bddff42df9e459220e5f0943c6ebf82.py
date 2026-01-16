import streamlit as st
from streamlit_option_menu import option_menu
from utils import load_data
from home import run_home
from eda import run_eda_home
from ml import run_ml_home

def main() :

    with st.sidebar:
        st.image('C:\\task\\images.jpeg')
        selected = option_menu('한강 이용방안', ['한강공원', '한강 주차장', '한강 지천길'],
                               icons=['house', 'file-bar-graph', 'map'], menu_icon='water', default_index=0)
    
    total_df = load_data('C:\\task\\한강공원 주차장 월별 이용 현황.csv')
   

    if selected == '한강공원' :
        run_home()
    elif selected == '한강 주차장' :
        run_eda_home(total_df)
    elif selected == '한강 지천길' :
        total_df3 = load_data('C:\\task\\한강지천길정리.csv')
        run_ml_home(total_df3)
    else :
        print('error')
        
if __name__ == "__main__" :
    main()

    
        
    