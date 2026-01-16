export class Tester{
	constructor(public firstName : string, public lastName : string){
		
	}
	
	getFullName(){
		return `${this.firstName} ${this.lastName}`; 
	}
}
