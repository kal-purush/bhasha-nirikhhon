// select items from the DOM model
// always calling with dot notation
const menuBtn = document.querySelector('.menu-btn');
const menu = document.querySelector('.menu');
const menuNav = document.querySelector('.menu-nav');
const menuBranding = document.querySelector('.menu-branding');
const navItems = document.querySelectorAll('.nav-item');

// set initial overlay
 
let showMenu = false;
// we now need an event listner!

menuBtn.addEventListener('click', toggleMenu);
// now create "toggleMenu"
function toggleMenu() {
    if (!showMenu) {
        menuBtn.classList.add('close');
        menu.classList.add('show');
        menuNav.classList.add('show');
        menuBranding.classList.add('show');
        // high order array method
        navItems.forEach(item => item.classList.add ('show'));

        // set menu state
        showMenu = true;

    } else {
        menuBtn.classList.remove('close');
        menu.classList.remove('show');
        menuNav.classList.remove('show');
        menuBranding.classList.remove('show');
        // high order array method
        navItems.forEach(item => item.classList.remove ('show'));

        // set menu state
        showMenu = false;

    }
}