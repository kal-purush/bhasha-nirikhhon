let button = document.querySelector('.burger')
let nav = document.querySelector('.nav')

button.addEventListener('click', function(){
    nav.classList.toggle('clicked')
})