import streamlit as st
import requests


# Добавить заголовок
st.title('Data Driven Engineering')

# Заголовок приложения
st.title('PCR product detection')

# Адрес сервера для обработки видео
# url = 'http://host.docker.internal:8000/classify'
url = 'http://127.0.0.1:8000/classify'


# Вводимый пользователем файл
uploaded_file = st.file_uploader("Загрузите изображение для анализа", type=['jpg'])

if uploaded_file is not None:
    st.write('Файл успешно загружен. Нажмите "Анализировать" для начала обработки.')
    
    # При нажатии кнопки "Анализировать" отправляем видео на сервер для обработки
    if st.button('Анализировать'):
        files = {'image': ('image.jpg', uploaded_file.read(), 'application/octet-stream')}
        response = requests.post(url, files=files)

        if response.status_code == 200:
            results = response.json()
            res_class = results['predicted_class']
            res_prob = (results['prediction_probabilities'][results['predicted_class']]) * 100
            st.write('Анализ завершен. Результаты:')            
            st.write(f"Класс на изображении: '{res_class}', с вероятностью {res_prob:.2f}%")

        else:
            st.write('Произошла ошибка при обработке.')
            st.write(response.text)
