class Saiyan {
	name: string;
	powerLevel: number;

	constructor(myName: string, myPower: number) {
		this.name = myName;
		this.powerLevel = myPower;
	}

	capturePlanets() {
		console.log("captured!");
	}
}

class Human {
	name: string;
	occupation: string;

	constructor(myName: string, myOccupation: string) {
		this.name = myName;
		this.occupation = myOccupation;
	}

	doNothing() {
		console.log("Nothing here to see");
	}
}


function saiyanTest(sentient: Human|Saiyan) {
	if (sentient instanceof Saiyan) {
		sentient.capturePlanets();
	}
	else if (sentient instanceof Human) {
		sentient.doNothing();
	}
}

saiyanTest(new Human("Son", "Goku"));




function listNames<T>(names: string | string[]) {
	if (typeof names == "string")
		console.log(names);        
	else
		for (name of names)
			listNames(name);
}

listNames("Saiyan");
listNames("James");