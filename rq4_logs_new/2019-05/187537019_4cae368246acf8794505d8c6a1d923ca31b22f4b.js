let lista_Cursos = [
	  { 
		id: 1,
		nombre: 'Microsoft Word',
		duracion: '30 horas',
		valor: 20000
	},{ 
		id: 2,
		nombre: 'Microsoft Excel',
		duracion: '60 horas',
		valor: 40000
	},{ 
		id: 3,
		nombre: 'Microsoft PowerPoint',
		duracion: '90 horas',
		valor: 60000
	}
];

let mostrarCursoRetardo = (aux,callback) => {
	setTimeout(function(){
		let res= ('Id del curso es: '+aux.id+'\nNombre del curso: '+aux.nombre+'\nDuracion de: '+aux.duracion+'\nCon un costo de: '+aux.valor+' COP\n');
		callback(res);
		},2000 * lista_Cursos.indexOf(aux));	   
}

let todosCursos = () => {
	console.log('Lista de todos los cursos: ');
	lista_Cursos.forEach( lista => mostrarCurso(lista));
}

let infoRegistro = (aux) =>{
	let res= ('Los datos de su registro son: \n'+'\nId del curso es: '+aux.id+'\nNombre del curso: '+aux.nombre+'\nDuracion de: '+aux.duracion+'\nCon un costo de: '+aux.valor+' COP\n');
	console.log(res);
}

module.exports = {
	lista_Cursos,
	mostrarCursoRetardo,
	todosCursos,
	infoRegistro
};