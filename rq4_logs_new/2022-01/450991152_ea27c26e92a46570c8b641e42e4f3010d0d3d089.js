const togglerBtn = document.querySelector("#togglerBtn");
const navContainer = document.querySelector('#navContainer');

document.addEventListener("DOMContentLoaded", () => {

    togglerBtn.addEventListener("click", () => {
        togglerBtn.classList.toggle("navbar__toggler--active");
        navContainer.classList.toggle("nav__container--active");

        const navContainerHeight = navContainer.scrollHeight;

        if(!navContainer.style.maxHeight) {
            navContainer.style.maxHeight = `${navContainerHeight}px`;
        } else {
            navContainer.style.maxHeight = null;
        }

    })

});