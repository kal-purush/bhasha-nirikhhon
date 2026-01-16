const app = {
    btn: document.querySelector('button'),
    text: document.getElementById('text'),
    box: document.querySelector('.box'),
    //^initialization
    init() {
        app.btnClickListener();
        app.hoverOverListener();
        app.hoverOutListener();
    },
    //^methods
    btnClickListener() {
        app.btn.addEventListener('click', app.handleAnimation);
    },
    handleAnimation() {
        app.text.classList.toggle('animate__fadeInLeft');
    },
    hoverOverListener() {
        const hoverText = document.querySelector('.box');
        hoverText.addEventListener('mouseover', app.handleHoverAnimation);
    },
    hoverOutListener() {
        const hoverText = document.querySelector('.box');
        hoverText.addEventListener('mouseout', app.handleHoverRemoveAnimation);
    },
    handleHoverAnimation() {
        const hoverText = document.querySelector('.hover-text');
        hoverText.textContent = 'Text hovered !'
        hoverText.classList.add('animate__animated', 'animate__delay-1s', 'animate__fadeInLeft');
    },
    handleHoverRemoveAnimation() {
        const hoverText = document.querySelector('.hover-text');
        hoverText.classList.add('animate__animated', 'animate__delay-1s', 'animate__fadeOutLeft');
    }
};

app.init();