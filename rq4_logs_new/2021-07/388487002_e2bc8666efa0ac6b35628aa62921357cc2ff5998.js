const NavBarElements = {
    sectionTrigger: document.getElementById("section-2"),
    navbar: document.querySelector(".navbar"),
    navbarItems: document.querySelectorAll(".navbar .navbar-item"),
    navbarBackground: document.querySelector(".navbar .navbar-item__background")
}

let once = true;
window.addEventListener('scroll', function () {
    const posTop = NavBarElements.sectionTrigger.getBoundingClientRect().top;
    NavBarElements.navbar.classList.toggle('hidden', posTop <= 800);
    if (once) {
        // first initialize //
        let initializedItem = NavBarElements.navbarItems[NavBarElements.navbarItems.length - 1]
        initializedItem.classList.add("is-active")
        NavBarElements.navbarBackground.style.width = initializedItem.offsetWidth + "px"
        NavBarElements.navbarBackground.style.left = initializedItem.offsetLeft + "px"
        //------------------//
        once = false
    }
});


let setActive = (ev) => {
    ev.preventDefault();
    let el = ev.target

    if (el.classList.contains("is-active")) {
        return false
    }
    for (let i = 0; i < NavBarElements.navbarItems.length; i++) {
        NavBarElements.navbarItems[i].classList.remove('is-active');
    }

    el.classList.add("is-active")
    NavBarElements.navbarBackground.style.width = el.offsetWidth + "px"

    let posY = el.offsetLeft; // левый отступ эл-та от родителя

    NavBarElements.navbarBackground.style.left = posY + "px"
}

NavBarElements.navbarItems.forEach((el) => {
    el.addEventListener('click', setActive)
})