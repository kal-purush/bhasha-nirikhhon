document.getElementById('dataForm').addEventListener('submit', function(event) {
    event.preventDefault();

    const name = document.getElementById('name').value;
    const email = document.getElementById('email').value;
    const age = document.getElementById('age').value;

    // Проверка на валидность данных
    if (name && email && age) {
        displayTables(name, email, age);
        fetchDataForTable3(); // Получаем данные для третьей таблицы
    } else {
        alert("Пожалуйста, заполните все поля.");
    }
});

function displayTables(name, email, age) {
    const tablesContainer = document.getElementById('tablesContainer');
    tablesContainer.style.display = 'block'; // Показываем контейнер с таблицами

    // Создание таблиц с данными
    const table1 = `
        <h3>Таблица 1: C++</h3>
        <table>
            <tr><th>Автор</th><td>Бьёрн Страуструп</td></tr>
            <tr><th>Год создания</th><td>Начало 1980-х</td></tr>
            <tr><th>Основное применение</th><td>Системное программирование, разработка ПО</td></tr>
        </table>
    `;

    const table2 = `
        <h3>Таблица 2: C#</h3>
        <table>
            <tr><th>Автор</th><td>Андерс Хейлсберг</td></tr>
            <tr><th>Год создания</th><td>2000 год</td></tr>
            <tr><th>Основное применение</th><td>Разработка приложений для Windows, веб-разработка</td></tr>
        </table>
    `;

    document.getElementById('table1').innerHTML = table1;
    document.getElementById('table2').innerHTML = table2;
}

async function fetchDataForTable3() {
    try {
        const response = await fetch('http://127.0.0.1:8000/home'); // URL вашего Mock JSON Server
        if (!response.ok) {
            throw new Error(`Ошибка HTTP: ${response.status}`);
        }

        const responseData = await response.json();
        const data = responseData.data;

        // Обработка данных для таблицы 3
        let rows = '';
        data.forEach(item => {
            rows += `
                <tr>
                    <td>${item.name}</td>
                    <td>${item.use}</td>
                </tr>
            `;
        });

        const table3 = `
            <h3>Таблица 3: Данные с сервера</h3>
            <table>
                <tr><th>Название</th><th>Использование</th>
                ${rows}
            </table>
        `;

        document.getElementById('table3').innerHTML = table3;

    } catch (error) {
        console.error('Ошибка при получении данных:', error);
        document.getElementById('table3').innerHTML = `
            <h3>Таблица 3: Ошибка при загрузке данных</h3>
            <p>${error.message}</p>
        `;
    }
}