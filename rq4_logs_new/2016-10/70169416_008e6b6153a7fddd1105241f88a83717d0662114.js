 /**
 * Realizado por Antonio Luque Bravo
 */
 {
 	function limita(evento,maximoCaracteres) {
		let teclado = evento || window.event;
		let tecla = teclado.charCode || teclado.keyCode;	
		let elemento = document.getElementById("texto");
		//	flechas		horizontales | retroceso |	suprimir.
		if (tecla ==37 || tecla==39 || tecla==8 || tecla == 46) {
			return true;
		}

		if(elemento.value.length >= maximoCaracteres) {
			return false;
		}
		else {
			return true;
		}
	}
	function comprobarCaracteresRestantes(maximoCaracteres){
		let textArea = document.getElementById("texto");
		let caracteresRestantes = maximoCaracteres - textArea.value.length;
		document.getElementById("caracteresRestantes").innerHTML = 
								'Te quedan ' + caracteresRestantes;
	}
}