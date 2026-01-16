//import ;

class Conversor {
	constructor() {
		
	}
	
	converterEmDecimal(numero, base = 2) {
		let numeroInvertido = numero.split('').map(Number).reverse();
		
		return numeroInvertido.reduce(
			(acumulador, valorAtual, index) => acumulador + valorAtual * Math.pow(base, index)
		);
	}
}

export default {
	Conversor
};