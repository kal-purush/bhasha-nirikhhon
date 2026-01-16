
let altura = 0
let largura = 0
let vidas = 1
let tempo = 10
let nivel = window.location.search
nivel  = nivel.replace('?','')
let criaMosquitoTempo = 1500

if(nivel === 'facil'){

	criaMosquitoTempo = 1500

} else if(nivel === 'normal'){

	criaMosquitoTempo = 1000

} else if(nivel === 'dificl'){
	
	criaMosquitoTempo = 750
}


function ajustaTamanhoPalcoJogo() {
	altura  = window.innerHeight
	largura = window.innerWidth
}
 
ajustaTamanhoPalcoJogo()

function posicaoRandomica(){

	//Verfica se ja existe o mosquito, caso exista será removido
	if(document.getElementById('mosquito')){
		document.getElementById('mosquito').remove()

		if(vidas > 3){
			window.location.href = 'game_over.html';
		}else{
				document.getElementById('v' + vidas).src = 'imagens/coracao_vazio.png'
				vidas++
		}
	}

		let posicaoX = Math.floor(Math.random() * largura) - 90
		let posicaoY = Math.floor(Math.random() * altura) - 90

		posicaoX = posicaoX < 0 ? 0 : posicaoX
		posicaoY = posicaoY < 0 ? 0 : posicaoY

		let mosquito = document.createElement('img')

		mosquito.src = 'imagens/mosca.png'
		mosquito.className = tamanhoAleartorio() + ' ' + ladoAleartorio()
		mosquito.style.left = posicaoY + 'px'
		mosquito.style.top  = posicaoX + 'px'
		mosquito.style.position = 'absolute'
		mosquito.id = 'mosquito'
		mosquito.onclick = function (){
			this.remove()
		}

		document.body.appendChild(mosquito)
}	

let cronometro = setInterval(function(){

	tempo -= 1

		if(tempo < 0){

			clearInterval(cronometro)
			clearInterval(criaMosquito)
			window.location.href = 'vitoria.html'

		}else{

			document.getElementById('cronometro').innerHTML = tempo
		}

		console.log(tempo)
}, 1000)

function tamanhoAleartorio(){

	let classe = Math.floor(Math.random() * 3)
	
	switch(classe){
		case 0 :
			return 'mosquito1'
		case 1 :
			return 'mosquito2'
		case 2 :
	}		return 'mosquito3'
}

function ladoAleartorio(){

	let lado = Math.floor(Math.random() * 2)

	switch(lado){
		case 0 :
			return 'ladoA'
		case 1 :
			return 'ladoB'		
	}
}

function reiniciaJogo(){
	window.location.href = 'app.html';
}

function iniciarJogo(){
	window.location.href = 'app.html?' + nivel 
}