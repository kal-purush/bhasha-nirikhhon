export function fechaMenu() {
    document.querySelectorAll('header nav ul li a').forEach(link => {
        link.addEventListener('click', () => {
            document.getElementById('nav_check').checked = false;
        });
    });
}