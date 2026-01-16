from PIL import Image
import requests
import streamlit as st
from streamlit_lottie import st_lottie
import webbrowser

st.set_page_config(page_title = 'My Webpage', page_icon = ':fire:', layout = 'wide')

def load_lottieurl(url):
    r = requests.get(url)
    if r.status_code != 200:
        return None
    return r.json()

def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html = True)

local_css("style/style.css")

#---- LOAD ASSETS ----
lottie_coding = load_lottieurl('https://assets5.lottiefiles.com/packages/lf20_es4p9zph.json')
img_walmart = Image.open("images/Walmart.jpg")
img_fifa = Image.open("images/FIfa.png")
img_sales = Image.open("images/Sales.png")

#----- HEADER SECTION -----
with st.container():
    st.header('Hi, I am Mritunjay :smile:')
    st.title('A Data Analyst from Canada ')
    st.write('I am passionate about finding ways to use Python, VBA, and SQL to be more efficient and effective in bussiness settings.')
    my_info = st.button('Learn More')
    if my_info:
        webbrowser.open('https://www.linkedin.com/in/mritunjay023/')


#---- What I Do----
with st.container():
    st.write("---")
    left_column, right_column = st.columns(2)
    with left_column:
        st.header("What I do")
        st.write("##")
        st.write(
            '''
            My passion is to find innovative solutions to the problems already existing in the world of Technology and Data by:
            - Analyzig trends and delinquecies using ad hoc analysis.
            - Developing relational databases using SQL, VBA for data Warehousing.
            - Using Streamlit to build interactive websites and web based dashboards that are user friendly.
            - Utilizig SK Learn library to develop AI and ML models. 
            - Creating meaningful reports and dashboards from complex datasets using Streamlit, Excel and Power BI.
            - Eliciting complex problems in the industy and developing crucial insights.
            - Applying best industry practices and ETL methodologies to maintain data marts.
            - Using normalization techniques to maintain relational databases.

            If this sounds interesting check out my GitHub page for more information.
            '''
        )
        st.write("[GitHub Page >](https://github.com/Mukul023)")
    with right_column:
        st_lottie(lottie_coding, height = 430, key= 'coding')

# ---- PROJECTS -----
with st.container():
    st.write("---")
    st.header('My Projects')
    st.write('##')
    image_column, text_column = st.columns((1,2))
    with image_column:
        st.image(img_walmart)
    with text_column:
        st.subheader("Walmart sales project")
        st.write(
            """
            There are many seasons that sales are significantly higher or lower than averages. If the company does not know about these seasons, it can lose too much money.
            Our main objective is to know how inclusion of holidays soars the sales in the store. 
            """
        )
        st.markdown("[More Details](https://github.com/Mukul023/Walmart-sales-project)")
with st.container():
    image_column, text_column = st.columns((1,2))
    with image_column:
        st.image(img_fifa)
    with text_column:
        st.subheader("Fifa Dataset Analysis")
        st.write(
            """
            This project is aimed towards helping a club manager towards better managing their players in term of fitness level, availability, preferred position to play,
            preferred foot to play and so much more. 
            """
        )
        st.markdown("[More Details](https://github.com/Mukul023/Fifa-Dataset-and-Analysis)")
with st.container():
    image_column, text_column = st.columns((1,2))
    with image_column:
        st.image(img_sales)
    with text_column:
        st.subheader("Sales and Wait time Analysis")
        st.write(
            """
            Here we are dealing with a dataset derived directly from an actual sandwich making shop that wants to know about the various wait times that the customers are facing
            in the shop and the correlation between all those variable times. On top of that, we provided them with a linear regression model for their future sales.

            """
        )
        st.markdown("[More Details](https://github.com/Mukul023/Sales-and-Wait-Time-Analysis)")
st.write("##")
st.write("For more interesting Data driven projects please visit my GitHut page by clicking the link below")
st.markdown("[GitHub](https://github.com/Mukul023)")

#---- CONTACT -----
with st.container():
    st.write("---")
    st.header("Get In Touch With Me! ")
    st.write("##")

    contact_form = """
    <form action="https://formsubmit.co/mukulpargotra27@gmail.com" method="POST">
        <input type = "hidden" name="_captcha" value="false">
        <input type="text" name="name" placeholder="Your Name" required>
        <input type="email" name="email" placeholder="Your Email" required>
        <textarea name="message" placeholder = "Your message here" required></textarea>
        <button type="submit">Send</button>
    </form>
    """
    left_column, right_column = st.columns(2)
    with left_column:
        st.markdown(contact_form, unsafe_allow_html = True)
    with right_column:
        st.empty()
