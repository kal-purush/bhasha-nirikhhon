
import { Component } from '@angular/core';
import { NgModel } from '@angular/forms';
import { Bike } from './bike.model';

@Component({
	selector: 'app-bike-component',
	templateUrl: './bike.component.html',
	styleUrls: ['./bike.component.css']
})

export class BikeComponent {

	bikes:Bike[];

	regNum:string ='';	// first character must be a capital letter
	brand:string = '';
	model:string = '';
	gearNum:string = ''; //max 2 
	type:string = ''; // one type between: road bike, mountain bike, BMX

	msg:boolean = false;

	constructor(){}

	createBike(){

	}

	validateRegNum(regNum:string):boolean{

		let result = +regNum.charAt(0);

		if(typeof result === 'number'){

			return true;
		}

		if(isNaN(result)){

			if(result === ''){

				return true;
			}
		}

		if (isNaN(result)) { 

			if (isNaN(Number(regNum.slice(1)))){

				return true;

			}
		}

		

		return false;
	}


	validateRegNum2(){

		this.regNum = this.regNum.trim();

		let param = this.regNum;

		if(param === ''){

			this.msg = false;

		} else {

			this.msg = this.validateRegNum(param);	
		}

	}

}