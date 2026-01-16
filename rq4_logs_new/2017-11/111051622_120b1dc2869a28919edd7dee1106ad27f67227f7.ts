

import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators } from '@angular/forms';

import { Ship } from './ship.model';


@Component({
	selector: 'app-ship-component',
	templateUrl: './ship.component.html',
	styleUrls: ['./ship.component.css']
})

export class ShipComponent implements OnInit{

	ships: Ship[] = [];
	shipForm: FormGroup;

	types:string[] = ['Tour Boat', 'Cargo Ship', 'Boat'];

	constructor(){}

	ngOnInit(){

		this.shipForm = new FormGroup({

			'plateNum': new FormControl(null, [Validators.required, Validators.pattern('[0-9]{3}-[A-Z]{2}')]),
			'brand': new FormControl(null, Validators.required),
			'model': new FormControl(null, Validators.required),
			'type': new FormControl(null, [Validators.required, this.typeValidator.bind(this)]),
			'maxSp': new FormControl(null, [Validators.required, Validators.pattern('^[0-9]+$')])

		});

		//this.shipForm.get('maxSp').setErrors({required: false});


		console.log(this.shipForm);
	}

	onSubmit(){
		//create ship
		console.log(this.shipForm);
		console.log("Estado Creacion "+ this.shipForm.valid);

		if(this.shipForm.valid){

			let ship = new Ship(
				this.shipForm.get('plateNum').value,
				this.shipForm.get('brand').value,
				this.shipForm.get('model').value,
				this.shipForm.get('type').value,
				+this.shipForm.get('maxSp').value);

			this.ships.push(ship);
			console.log("succes");

		}

		else{

			this.shipForm.markAsDirty();	
		}

	}

	typeValidator(control:FormControl):{[key:string]:boolean}{

		//if the type string we pass from the form is not in types throw error
		if(this.types.indexOf(control.value) === -1){
			//console.log("se ejecuta");
			return {'typeNotAllowed': true};

		}

		return null;
	}


}