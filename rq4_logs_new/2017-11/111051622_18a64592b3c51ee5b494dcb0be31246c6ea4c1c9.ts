

import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators } from '@angular/forms';

import { Car } from './car.model';


@Component({
	selector: 'app-car-component',
	templateUrl: './car.component.html',
	styleUrls: ['./car.component.css']
})

export class CarComponent implements OnInit{

	cars: Car[] = [];
	carForm: FormGroup;

	types:string[] = ['Electric', 'Sport', 'HatchBack', 'Family'];
	motors:string[] = ['1.0', '1.4', '1.8', '2', '2.5'];

	sportSelected:boolean=false;

	constructor(){}

	ngOnInit(){

		this.carForm = new FormGroup({

			'plateNum': new FormControl(null, [Validators.required, Validators.pattern('[A-Z]{3}-[0-9]{3}[A-Z]{1}')]),
			'brand': new FormControl(null, Validators.required),
			'model': new FormControl(null, Validators.required),
			'type': new FormControl(null, [Validators.required, this.typeValidator.bind(this)]),
			'motor': new FormControl(null, [Validators.required, this.motorValidator.bind(this)]),
			'maxSp': new FormControl( {value: '', disabled: true}, [Validators.required, Validators.pattern('^[0-9]+$')])

		});

		this.carForm.get('maxSp').setErrors({required: false});
	

		console.log(this.carForm);
	}

	onSubmit(){
		//create car
		console.log(this.carForm);
		console.log("Estado Creacion "+ this.carForm.valid);

		if(this.carForm.valid){

			let car = new Car(
				this.carForm.get('plateNum').value,
				this.carForm.get('brand').value,
				this.carForm.get('model').value,
				this.carForm.get('type').value,
				+this.carForm.get('motor').value,
				this.carForm.get('maxSp').value);

			this.cars.push(car);
			console.log("succes");

		}

		else{

			this.carForm.markAsDirty();	
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

		//if the motor string we pass from the form is not in types throw error
		if(this.motors.indexOf(control.value) === -1 ){
			//console.log("value of motor: " + control.value);
			return {'motorNotAllowed': true};
		}

		return null;
	}

	//now we need to get check for the value of type so whenever Electricd is selected
	//motors is modified 

	onChange(){

		let value = this.carForm.get('type').value;

		if(value === 'Electric'){

			this.motors = ['1.0','1.4'];
			this.carForm.get('motor').reset();
		}
		else {

			this.motors =  ['1.0', '1.4', '1.8', '2', '2.5'];

		}

		if(value === 'Sport'){

		
			this.carForm.get('maxSp').reset({value: '', disabled: false});
			this.carForm.get('maxSp').setErrors({required:true});
			//this.carForm.get('maxSp').updateValueAndValidity();
			//this.carForm.get('maxSp').setErrors({pattern:false});
			console.log(this.carForm.get('maxSp'));	
		//	console.log(this.carForm);	
		}
		else{
			
			this.carForm.get('maxSp').reset({value: '', disabled: true});
			this.carForm.get('maxSp').setErrors({required:false});

		}


	}



}