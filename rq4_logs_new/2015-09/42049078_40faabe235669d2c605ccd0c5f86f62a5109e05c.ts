// an interface defines the SHAPE of an entity

interface IArm {
	id: number;
	description: string;
}

interface ILeg {
	id: number;
	description: string;
}

interface IHead {
	id: number;
	description: string;
	hair: string;
	brain?: {} // <- make this optional: this property might not be there!
}

interface IHuman {
	head: IHead;
	leftArm: IArm;
	rightArm: IArm;
	leftLeg: ILeg;
	rightLeg: ILeg;
}

var alessandro = <IHuman>{};
alessandro.head = <IHead>{};
alessandro.leftArm = <IArm>{};
alessandro.rightArm = <IArm>{};
alessandro.leftLeg = <ILeg>{};
alessandro.rightLeg = <ILeg>{};

// WARNING let's mess up with the human body! (Structural Typing - Duck Typing)















// interface can describe functions: function types (delegates in C#)

interface decision{
	(head: IHead): Boolean
}

var canIthink: decision = function(head: IHead) { return true; }

// interface can describe arrays: array types / dictionaries

interface StringDictionary {
	[index: string]: string; // index can be number or string
}
// WARNING: all additional properties must match the return type of the indexer 

// inheritance

interface ISuperHero extends IHuman {
	specialPower: string;
}

// Hybrid types: 
// JavaScript id dynamic and flexible and interfaces have to describe all those objects
// that act both as function and objects.

interface Timer {
	(interval: number): string;
	interval: number;
	reset(): void;
}