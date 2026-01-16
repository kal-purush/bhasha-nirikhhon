document.addEventListener("DOMContentLoaded", insereSumario) 

function insereSumario(){
	const ol = document.createElement('ol')
	ol.id = 'sumário'
	document.body.prepend(ol)
	const secoes = document.querySelectorAll('h2')
	for(let i=0; i<secoes.length; i++) {
		const li = criarItem(`#sec${i+1}`, secoes[i].textContent)
		ol.appendChild(li) 
		alterarH2(secoes[i], i+1) 
	}
}

function alterarH2(elem, secnum){ 
	inserirNumero(elem, secnum)
	elem.id = `sec${secnum}`
	const voltar = criarLink('#', 'voltar') 
	inserirDepois(elem, voltar)
}

function criarItem(href, texto){
	const li = document.createElement('li')
	const linque = criarLink(href,texto)
	li.appendChild(linque)
	return li
}

function inserirNumero(elem, numero) {
	elem.textContent = `${numero}. ${elem.textContent}`
}

function criarLink(href, texto) {
	const linque = document.createElement('a') 
	linque.setAttribute('href', href) 
	linque.textContent = texto
	return linque 
}
	
function inserirDepois(de, elem) {
	const proximoIrmao = de.nextElementSibling;
	de.parentElement.insertBefore(elem,proximoIrmao)
}