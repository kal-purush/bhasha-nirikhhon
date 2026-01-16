//nav
const nav = document.querySelector('.nav')
const openNav = document.querySelector('#opennav')
const closeNav = document.querySelector('#closenav')

function openNavBtn(){
    nav.style.display = 'flex'
}

openNav.addEventListener('click', openNavBtn)

function closenNavBtn(){
    nav.style.display = 'none'
}

closeNav.addEventListener('click', closenNavBtn)