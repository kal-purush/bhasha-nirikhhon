// Массив с фразами
const quotes = [
    "Жизнь не ждет, когда ты будешь готов.",
    "Каждый день - новый шанс изменить свою жизнь.",
    "Смелость - это не отсутствие страха, а способность двигаться вперед, несмотря на него.",
    "Ваше время ограничено, не тратьте его, живя чужой жизнью.",
    "Мечтайте большие мечты, и будьте готовы к большим изменениям.",
    "Успех - это сумма маленьких усилий, повторяемых день за днем.",
    "Не бойтесь делать ошибки, бойтесь не попробовать.",
    "Только те, кто рискует идти далеко, могут узнать, насколько далеко можно зайти.",
    "Каждый день - это новая возможность стать лучше.",
    "Секрет успеха - это начинать с того, что вы можете делать, с тем, что у вас есть."
];

// Функция для получения текущей даты в формате 'YYYY-MM-DD'
function getCurrentDate() {
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

// Функция для выбора случайной фразы
function getRandomQuote() {
    const today = getCurrentDate();
    let lastUpdateDate = localStorage.getItem('lastUpdateDate');

    // Если дата последнего обновления отличается от сегодняшней, обновляем фразу
    if (lastUpdateDate !== today) {
        // Выбираем случайную фразу
        const randomIndex = Math.floor(Math.random() * quotes.length);
        const randomQuote = quotes[randomIndex];

        // Сохраняем выбранную фразу и дату последнего обновления
        localStorage.setItem('quoteOfTheDay', randomQuote);
        localStorage.setItem('lastUpdateDate', today);

        return randomQuote;
    } else {
        // Если фраза уже обновлялась сегодня, возвращаем сохраненную фразу
        return localStorage.getItem('quoteOfTheDay') || quotes[0];
    }
}

// Функция для отображения фразы на странице
function displayQuote() {
    const quoteElement = document.getElementById('Quota');
    if (quoteElement) {
        quoteElement.innerText = getRandomQuote();
    }
}

// Функция для отправки уведомления
function sendNotification() {
    // Запрашиваем разрешение на отправку уведомлений
    if (Notification.permission === 'default') {
        Notification.requestPermission().then(permission => {
            if (permission === 'granted') {
                // Отправляем уведомление
                const quote = document.getElementById('Quota').innerText;
                new Notification('Ваша фраза на сегодня:', {
                    body: quote,
                    icon: 'icon.png' // путь к иконке уведомления
                });
            }
        });
    } else if (Notification.permission === 'granted') {
        // Отправляем уведомление, если разрешение уже предоставлено
        const quote = document.getElementById('Quota').innerText;
        new Notification('Ваша фраза на сегодня:', {
            body: quote,
            icon: 'icon.png' // путь к иконке уведомления
        });
    }
}

// Функция для установки уведомления на определенное время
function scheduleNotification() {
    const now = new Date();
    const hours = now.getHours();
    const minutes = now.getMinutes();
    const seconds = now.getSeconds();
    const targetHour = 20; // Час для отправки уведомления (6 утра)
    
    if (hours >= targetHour && minutes >= 0) {
        // Отправляем уведомление если текущее время после целевого времени
        sendNotification();
    } else {
        // Рассчитываем сколько времени до целевого времени
        const targetTime = new Date();
        targetTime.setHours(targetHour, 0, 0, 0);
        const timeToWait = targetTime - now;

        // Устанавливаем таймер для отправки уведомления
        setTimeout(() => {
            sendNotification();
        }, timeToWait);
    }
}

// Вызов функции при загрузке страницы
window.onload = () => {
    displayQuote();
    scheduleNotification();
};