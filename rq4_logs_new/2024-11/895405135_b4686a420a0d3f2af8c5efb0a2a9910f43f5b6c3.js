// Menu click event handling
let menu = document.getElementById('menu-icon');
let navlist = document.querySelector('.navlist');

menu.onclick = () => {
    menu.classList.toggle('bx-x');
    navlist.classList.toggle('open');
};

// ScrollReveal configuration
const sr = ScrollReveal({
    distance: '65px',
    duration: 2600,
    delay: 450,
    reset: true
});

// Reveal elements on scroll
sr.reveal('.hero-text', { origin: 'top', delay: 200 });
sr.reveal('.hero-image', { origin: 'top', delay: 400 });
sr.reveal('.icons', { origin: 'left', delay: 500 });
sr.reveal('.scroll-down', { origin: 'right', delay: 500 });