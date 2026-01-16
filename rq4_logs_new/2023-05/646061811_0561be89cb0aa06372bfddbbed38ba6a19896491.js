const button_up = document.querySelector('.button_up');

button_up.addEventListener('mousedown', function() {
    window.scrollTo(0, 0)
});

window.addEventListener('scroll', function() {
    if (window.pageYOffset >= 70) {
        button_up.classList.add('button_up_show');
    } else {
        button_up.classList.remove('button_up_show');
    }
});