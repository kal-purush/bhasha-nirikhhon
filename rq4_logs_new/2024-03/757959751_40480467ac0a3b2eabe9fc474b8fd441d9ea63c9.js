/* Arquivo JS + alterar CSS */

let titulo = document.querySelector('h1')
titulo.textContent = 'Aula 09 Manipular CSS'
titulo.innerHTML = 'Aula Manipular CSS'
let imagem = document.querySelector('#foto')
imagem.setAttribute('src', 'img/dance.gif')
imagem.setAttribute('width', '280px')

/* MANIPULAR CSS */
document.querySelector('h1').style.color = "red";
titulo.style.color = "red";
titulo.style.backgroundColor = "#000";
titulo.style.borderBottom = "2px solid red";
titulo.style.padding = "0.625rem";
titulo.style.borderRadius = "5px";

let box = document.querySelectorAll('.box')
box[0].setAttribute('class', 'escura')
box[0].removeAttribute('class')

/////// MODOS DE COR ///////
let tela = document.querySelector('main')

let btnDark = document.querySelector('#btdark')

let btnLight = document.querySelector('#btlight')

let btnBlue = document.querySelector('#btblue')

let btnYellow = document.querySelector('#btyellow')

let btnPink = document.querySelector('#btpink')

btnDark.addEventListener('click', modoDark)
btnLight.addEventListener('click', modoLight)
btnBlue.addEventListener('click', modoBlue)
btnYellow.addEventListener('click', modoYellow)
btnPink.addEventListener('click', modoPink)

function modoDark() {
    //event.preventDefault();
    console.log('modo dark')
    imagem.setAttribute('src', 'img/venom.gif')
    tela.classList.remove("light");
    tela.classList.remove("azul");
    tela.classList.remove("amarelo");
    tela.classList.remove("rosa");
    tela.classList.add("dark");
}

function modoLight() {
    //event.preventDefault();
    console.log('modo light')
    imagem.setAttribute('src', 'img/dance.gif')
    tela.classList.remove("dark");
    tela.classList.remove("azul");
    tela.classList.remove("amarelo");
    tela.classList.remove("rosa");
    tela.classList.add("light");
}

function modoBlue() {
    //event.preventDefault();
    console.log('modo Azul')
    imagem.setAttribute('src', 'img/reigelado.gif')
    tela.classList.remove("light");
    tela.classList.remove("amarelo");
    tela.classList.remove("rosa");
    tela.classList.remove("dark");
    tela.classList.add("azul");
}

function modoYellow() {
    //event.preventDefault();
    console.log('modo Amarelo')
    imagem.setAttribute('src', 'img/minion.gif')
    tela.classList.remove("light");
    tela.classList.remove("rosa");
    tela.classList.remove("azul");
    tela.classList.remove("dark");
    tela.classList.add("amarelo");
}

function modoPink() {
    //event.preventDefault();
    console.log('modo Rosa')
    imagem.setAttribute('src', 'img/kirby.gif')
    tela.classList.remove("light");
    tela.classList.remove("amarelo");
    tela.classList.remove("azul");
    tela.classList.remove("dark");
    tela.classList.add("rosa");
}

/*
titulo.addEventListener('click', mudarId)
function mudarId() {
    titulo.id = 'titulo'
    console.log(titulo.id)
}
*/