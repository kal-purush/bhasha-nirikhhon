const buttonMenuMobile = document.getElementsByClassName('menu-mobile-area')[0]
buttonMenuMobile.addEventListener('click', showMenu = () => {
    document.querySelector('.ul-header').classList.toggle('show-ul')
})