let page = document.querySelector('.page');
let themeButtom = document.querySelector('.theme-button');
themeButtom.onclick = function () {
    page.classList.toggle('light-theme');
    page.classList.toggle('dark-theme');
};