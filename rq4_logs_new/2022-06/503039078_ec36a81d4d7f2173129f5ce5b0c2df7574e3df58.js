let hamburger = document.querySelector('.hamburger')
let efeito = document.querySelector('.sidebar')
let botao1= document.querySelector('.hamburger')

botao1.classList.remove('invisivel1');
let botao2 = document.querySelector('.hamburger2')





hamburger.addEventListener('click',function(){
    efeito.classList.remove('invisivel')
    botao1.classList.add('invisivel1')
    botao2.classList.remove('invisivel2')
})

botao2.addEventListener('click',function(){
    efeito.classList.add('invisivel')
    botao1.classList.remove('invisivel1')
    botao2.classList.add('invisivel2')
})
