var alturaNavegador = 0
var larguraNavegador = 0
var vidas = 1
var tempo = 20
var criaMosquitoTempo = 3000

var pontuacao = 0

var difuculdade = window.location.search

if (difuculdade === '?facil') {
    criaMosquitoTempo = 3000
} else if (difuculdade === '?medio') {
    criaMosquitoTempo = 2000
} else {
    criaMosquitoTempo = 1000
}

function ajustaTamanhoTela() {
    alturaNavegador = window.innerHeight
    larguraNavegador = window.innerWidth

    console.log("Tamanho da tela -> " + "Altura: " + alturaNavegador + " | " + "Largura: " + larguraNavegador)
    console.log('--------------------------------------------------------')
}
ajustaTamanhoTela()


var cronometro = setInterval(function () {

    tempo = tempo - 1
    if (tempo < 0) {
        window.location.href = 'assets/telas/vitoriaJogo.html' + difuculdade
    }
    document.getElementById('cronometro').innerHTML = tempo
}, 1000)

function posicaoRandomicaMosquito() {
    
    if (document.getElementById('mosquito')) {
        document.getElementById('mosquito').remove()

        if (vidas > 3) {
            window.location.href = 'assets/telas/fimJogo.html'  + difuculdade
        } else {
            document.getElementById('vida' + vidas).src = "assets/img/coracao_vazio.png"
            vidas++
        }
    }

    var posicaoX = Math.random()
    var posicaoY = Math.random()

    posicaoX = posicaoX * larguraNavegador
    posicaoY = posicaoY * alturaNavegador

    posicaoX = Math.floor(posicaoX) - 90
    posicaoY = Math.floor(posicaoY) - 90

    if (posicaoX < 0) {
        posicaoX = 0
    } else {
        posicaoX = posicaoX
    }

    if (posicaoY < 0) {
        posicaoY = 0
    } else {
        posicaoY = posicaoY
    }

    console.log("Vertical: " + posicaoY + " Horizontal: " + posicaoX)

    var mosquito = document.createElement('img')
    mosquito.src = 'assets/img/mosquito.png'
    mosquito.className = tamanhoAleatorioMosquito() + ' ' + ladoAleatorioMosquito()
    mosquito.style.left = posicaoX + 'px'
    mosquito.style.top = posicaoY + 'px'
    mosquito.style.position = 'absolute'
    mosquito.id = 'mosquito'
    mosquito.onclick = function () {
        this.remove()
        pontuacao++
        document.getElementById('pontuacao').innerHTML = pontuacao
    }

    document.body.appendChild(mosquito)

    console.log('O mosquito gerado é o: ' + tamanhoAleatorioMosquito())
    console.log('O lado do mosquito é: ' + ladoAleatorioMosquito())
    console.log('--------------------------------------------------------')
}

function tamanhoAleatorioMosquito() {
    //Gerando valores, que podem ser: 0, 1 e 2
    var classeTamanhoMosquito = Math.floor(Math.random() * 3)

    //Verifica qual o valor da classe e atribui a um determinado tipo/nome de mosquito
    //Quando se usa return, o break não é necessario por o return já retorna um valor.
    switch (classeTamanhoMosquito) {
        case 0:
            return 'mosquito1'
        case 1:
            return 'mosquito2'
        case 2:
            return 'mosquito3'
    }

}

function ladoAleatorioMosquito() {

    var classeLadoMosquito = Math.floor(Math.random() * 2)

    switch (classeLadoMosquito) {
        case 0:
            return 'mosquitoLadoA'
        case 1:
            return 'mosquitoLadoB'
    }
}
