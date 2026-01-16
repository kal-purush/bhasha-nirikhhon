// Importar el núcleo de Angular
import  {Component, OnInit} from 'angular2/core';
//Importar libreria estandar para enrutamiento
import {Router, RouteParams} from 'angular2/router';
//Componente estructural
import {RestauranteService} from '../services/restaurante.service';
//Importar componente local Restaurante
import {Restaurante} from '../model/restaurante';

// Decorador @Component, confiración del componente
@Component ({
	selector: 'restaurantes-nuevo',
	templateUrl: 'app/view/restaurantes-nuevo.html',
	providers: [RestauranteService]
})


// Clase del componente donde iran los datos y funcionalidades
export class RestaurantesNuevo implements OnInit{
	private id_actual:string;
	private restaurante:Restaurante[];
	private status:string;
	private errorMessage:string;

	constructor(
			private _service: RestauranteService,
			private _routeParams: RouteParams,
			private _router: Router
		){}

	ngOnInit(){
		console.log('Componente RestaurantesNuevo cargado');
		
	}
}