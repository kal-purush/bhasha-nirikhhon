const themeButton = document.getElementById('themeButton');

// Load saved theme from local storage
if (localStorage.getItem('theme') === 'light') {
    document.body.classList.add('light-mode');
    themeButton.textContent = '🌙 Dark Mode';
}

// Toggle Theme
themeButton.addEventListener('click', () => {
    document.body.classList.toggle('light-mode');

    if (document.body.classList.contains('light-mode')) {
        themeButton.textContent = '🌙 Dark Mode';
        localStorage.setItem('theme', 'light');
    } else {
        themeButton.textContent = '☀ Light Mode';
        localStorage.setItem('theme', 'dark');
    }
});