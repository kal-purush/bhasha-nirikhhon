/**
 * Situation:
 * More than 1 functions/data requires structure that requires some necessary and optional properties
 * structure (person)
 * 	name, age, gender, profession, email, salary etc.
 */

enum Gender{male,female};
/**
 * if the person's property required in the definePerson function, we have to explicitly have to initiat
 * these properties to fullfil "duck typing" or "structural subtyping" by type-checker.
 * or if we want to use these one more functions then we have to code again and again.
 */
function definePerson(person:{name:string, age:number,gender:Gender, profession:string,email?:string}){
	
	// this.age
	var pronoun: string[]=["he","she"];
	var prefix: string[]=["Mr.","Ms."];
	
	let introduction = prefix[person.gender]+" "+person.name+" is "+person.age+ " years old and "+pronoun[person.gender]+" is "+person.profession;
	console.log(introduction);
}

// creating person object
var person1 = {name:"Sheryar Shirazi",age:23, gender:Gender.male,profession:"Developer",email:"sheryarshirazi@gmail.com"};
var person2 = {name:"Ahmar Ansari",age:26, gender:Gender.male,contact:"03548456987"};
var person3 = {name:"Naheed ur Rehman",age:38, gender:Gender.female,profession:"Chef"};


definePerson(person1);

// person2 has not all necessary properties.
// definePerson(person2);

definePerson(person3);
/**
 * So the solution of the above situation is to make interface of person.
 */
interface Person{
	name:string, age:number,gender:Gender, profession:string,email?:string
}

interface ISO900{
	medicalAllaonce:number, conveinceAllaonce:number
}
interface HajjFacility{
	expenses:number;
	passport:boolean;
	visa:boolean;
	maritialStatus:boolean;
}

function personIntro(person:Person){
	
	// this.age
	var pronoun: string[]=["he","she"];
	var prefix: string[]=["Mr.","Ms."];
	
	let introduction = prefix[person.gender]+" "+person.name+" is "+person.age+ " years old and "+pronoun[person.gender]+" is "+person.profession;
	console.log(introduction);
}

personIntro(person1);
// personIntro(person2);
personIntro(person3);

class Employee implements Person,ISO900{
	id:number;
	name:string;
	age:number;
	gender:Gender;
	profession:string;
	salary:number;
	grad:string;
	
	//ISO components
	medicalAllaonce:number;
	conveinceAllaonce:number;
	
	constructor(i:number,n:string,a:number,g:Gender,p:string,s:number,gr:string){
		this.id=i;
		this.name=n;
		this.age=a;
		this.gender=g;
		this.profession=p;
		this.salary=s;
		this.grad=gr;
	}
	
	hajjEligible(hajjDetail:HajjFacility,branch:string):boolean{
		 return(this.age>50 && (this.salary*12)<hajjDetail.expenses && branch=='main' );
	}
}

var employee1 = new Employee(788,"Ahmad Lashari",47,Gender.male,"Field Manager",35000,"E");
var employee2 = new Employee(650,"Mehjabeen Noor",55,Gender.female,"General Reporter",25000,"G");

if (employee1.hajjEligible({expenses:450000,passport:true,visa:true,maritialStatus:true},'main')) {
	console.log("Congrats! "+employee1.name+" of id # "+employee1.id+ " is eligible to get Hajj package this year.");
}else{
	console.log("Sorry ! "+employee1.name+" of id # "+employee1.id+ " is not eligible to get Hajj package this year.");
}
if (employee2.hajjEligible({expenses:450000,passport:true,visa:true,maritialStatus:true},'main')) {
	console.log("Congrats! "+employee2.name+" of id # "+employee2.id+ " is eligible to get Hajj package this year.");
}else{
	console.log("Sorry ! "+employee2.name+" of id # "+employee2.id+ " is not eligible to get Hajj package this year.");
}