// Отримуємо кнопку "Back" і додаємо обробник події для повернення на попередню сторінку
const backButton = document.getElementById('back-button');
backButton.addEventListener('click', () => {
    window.history.back();
});

// Отримуємо параметри URL і розпаковуємо дані з параметра 'data'
const urlParams = new URLSearchParams(window.location.search);
const data = JSON.parse(urlParams.get('data'));

// Отримуємо контейнер, в який будемо вставляти деталі поста
const container = document.getElementById('post-details-container');

// Створюємо елемент <p> для заголовку поста та додаємо його до контейнера
const title = document.createElement('p');
title.textContent = data.title;
title.classList.add('p-post-details-container');

// Створюємо елемент <p> для тіла поста та додаємо його до контейнера
const body = document.createElement('p');
body.textContent = data.body;
body.classList.add('p-post-details-container');

// Додаємо стилізований клас до контейнера
container.appendChild(title);
container.appendChild(body);
container.classList.add('post-details-container');


// Виконуємо запит для отримання коментарів до даного поста і вставляємо їх до списку коментарів
fetch(`https://jsonplaceholder.typicode.com/posts/${data.id}/comments`)
    .then(response => response.json())
    .then(comments => {
        const commentsList = document.getElementById('comments-list');
        commentsList.innerHTML = '';
        comments.forEach(comment => {
            const li = document.createElement('li');
            const name = document.createElement('p');
            name.innerHTML = `<strong>Name:</strong> ${comment.name}`;
            const email = document.createElement('p');
            email.innerHTML = `<strong>Email:</strong> ${comment.email}`;
            const body = document.createElement('p');
            body.innerHTML = `<strong>Comment:</strong> ${comment.body}`;

            // Додаємо дані коментаря до елементу <li> та вставляємо їх до списку коментарів
            li.appendChild(name);
            li.appendChild(email);
            li.appendChild(body);
            commentsList.appendChild(li);
        });
    })
    .catch(error => console.error(error));