
import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators } from '@angular/forms';

import { Motorcycle } from './motorcycle.model';


@Component({
	selector: 'app-motorcycle-component',
	templateUrl: './motorcycle.component.html',
	styleUrls: ['./motorcycle.component.css']
})

export class MotorcycleComponent implements OnInit{

	motorcycles: Motorcycle[] = [];
	motorCycleForm: FormGroup;

	types:string[] = ['Electric', 'Chopper', 'Street', 'Ninja'];
	motors:string[] = ['100', '125', '180', '250', '500', '1000'];

	errors:boolean[] = [false,false,false,false,false];
	errorStatus:string[] = ['','','','',''];

	constructor(){}

	ngOnInit(){

		this.motorCycleForm = new FormGroup({

			'plateNum': new FormControl(null, [Validators.required, Validators.pattern('[A-Z]{3}-[0-9]{3}')]),
			'brand': new FormControl(null, Validators.required),
			'model': new FormControl(null, Validators.required),
			'type': new FormControl(null, [Validators.required, this.typeValidator.bind(this)]),
			'motor': new FormControl(null, [Validators.required, this.motorValidator.bind(this)])

		});

	}

	onSubmit(){
		//create Motorcycle
		console.log(this.motorCycleForm);
		console.log("Estado Creacion "+ this.motorCycleForm.valid);

		if(this.motorCycleForm.valid){

			console.log("succes");

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


	motorValidator(control:FormControl):{[key:string]:boolean}{

		//if the motorCC string we pass from the form is not in types throw error
		if(this.motors.indexOf(control.value) === -1 ){
			console.log("value of motor: " + control.value);
			return {'motorCCNotAllowed': true};
		}

		return null;
	}

	//now we need to get check for the value of type so whenever Ninja is selected
	//motors is modified 

	onChange(){

		let value = this.motorCycleForm.get('type').value;
		this.motors = ( value === 'Ninja')?['500','1000']:['100', '125', '180', '250', '500', '1000'];
		this.motorCycleForm.patchValue({
			'motor': (( value === 'Ninja')? '': value)
		});
	}

	


}