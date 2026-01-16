let qtdCartas = 0;
let ordemCartas = [];
let cartaClicada = [];
let cartasViradas = [];
let pares = 0;
let contagemClicks = 0;

let figuraCartas = [
    '<image src="./projeto__parrots__imagens/Arquivos Úteis - Projeto 04 - Parrot Card Game/bobrossparrot.gif" alt="">',
    '<image src="./projeto__parrots__imagens/Arquivos Úteis - Projeto 04 - Parrot Card Game/explodyparrot.gif" alt="">',
    '<image src="./projeto__parrots__imagens/Arquivos Úteis - Projeto 04 - Parrot Card Game/fiestaparrot.gif" alt="">',
    '<image src="./projeto__parrots__imagens/Arquivos Úteis - Projeto 04 - Parrot Card Game/metalparrot.gif" alt="">',
    '<image src="./projeto__parrots__imagens/Arquivos Úteis - Projeto 04 - Parrot Card Game/revertitparrot.gif" alt="">',
    '<image src="./projeto__parrots__imagens/Arquivos Úteis - Projeto 04 - Parrot Card Game/tripletsparrot.gif" alt="">',
    '<image src="./projeto__parrots__imagens/Arquivos Úteis - Projeto 04 - Parrot Card Game/unicornparrot.gif" alt="">' 
];

function comparador() { 
	return Math.random() - 0.5; 
}

function quantidadeCartas(){
    do {
        qtdCartas = Number(prompt("Digite a quantidade de cartas para o jogo:\n(numero par de 4 a 14)"));
        if(!(qtdCartas === 4 || qtdCartas === 6 || qtdCartas === 8 || qtdCartas === 10 || qtdCartas === 12 || qtdCartas === 14)){
            quantidadeCartas();
        } 
    } while (!qtdCartas);
}

function cartasJogo(){
    let desenhoCarta;
    for(let i = 0; i < (qtdCartas / 2); i++){
        desenhoCarta = figuraCartas[i];
        ordemCartas.push(desenhoCarta);
        ordemCartas.push(desenhoCarta);
    }
    ordemCartas.sort(comparador);
}

function insereCartas(){
    const divCards = document.querySelector('.cards');

    for(let i = 0; i < qtdCartas; i++){
        divCards.innerHTML += `
        <div data-test="card" class="slotCarta" onclick="virar(this)">
            <div data-test="face-down-image" class="cartaEstilo carta1">                
                <image src="./projeto__parrots__imagens/Arquivos Úteis - Projeto 04 - Parrot Card Game/back.png" alt="">
            </div>
            <div data-test="face-up-image" class="cartaEstilo back-face carta2">
                ${ordemCartas[i]}
            </div>
            </div>
        `;
    }
}

function virar(carta){
    contagemClicks++;

    if(cartaClicada.length > 1){
        cartaClicada.pop();
        cartaClicada.pop();
    }

    cartaClicada.push(carta);
    cartasViradas.push(carta.innerHTML);

    if(cartasViradas.length > 1){
        if(cartasViradas[0] === cartasViradas[1]){
            console.log('iguais');
            pares++;
            
            cartasViradas.pop();
            cartasViradas.pop();
        } else {
            console.log('diferentes');
            cartasViradas.pop();
            cartasViradas.pop();
            setTimeout(desvira, 1000);
        }
    }

    const carta1 = carta.querySelector(".carta1");
    carta1.classList.toggle("front");

    const carta2 = carta.querySelector(".carta2");
    carta2.classList.toggle("back");

    if(pares === qtdCartas / 2) {
        alert(`Você ganhou em ${contagemClicks} jogadas!`);
    }
}

function desvira(){
    let carta1 = cartaClicada[0].querySelector(".carta1");
    carta1.classList.toggle("front");

    let carta2 = cartaClicada[0].querySelector(".carta2");
    carta2.classList.toggle("back");

    carta1 = cartaClicada[1].querySelector(".carta1");
    carta1.classList.toggle("front");

    carta2 = cartaClicada[1].querySelector(".carta2");
    carta2.classList.toggle("back");
}


// INICIO DO JOGO

figuraCartas.sort(comparador);
quantidadeCartas();
cartasJogo();
insereCartas(); 