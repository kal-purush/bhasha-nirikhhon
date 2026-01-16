var open;
document.addEventListener('DOMContentLoaded', onReady);

function onReady () {
    const toggleBar = document.getElementById('togglingBar');
    const toggleMenu = document.getElementById('toggleMenu');
    toggleBar.addEventListener('mouseover', onMouseOver); //mouseover
    toggleMenu.addEventListener('mouseout', onMouseOut); //mouseover
}

function onMouseOver (event) {
    const divMenu = document.querySelector('.headerNav__menu');
    const isActive = document.querySelector('.headerNav__menu--active320');
    if (divMenu && !isActive) {
        divMenu.classList.toggle('headerNav__menu--active320');
    }
}
function onMouseOut (event) {
    const divMenu = document.querySelector('.headerNav__menu');
    if (divMenu) {
        divMenu.classList.toggle('headerNav__menu--active320');
    }
}