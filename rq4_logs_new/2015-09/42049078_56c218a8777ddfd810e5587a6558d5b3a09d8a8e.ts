// classes implements the behaviors of an entity
// allows for:
// - accessors (get, set) [ES5+]
// - modifiers: public, private, protected
// - constructor
// - inheritable
// - static properties
//
// Classes also define Types (they have 2 sides):
// - instance side (the properties involved in structural typing)
// - static side  (constructor and static prps)
	
interface ILogger {
	name: string;
	log(message: string): void;
}

class ConsoleLogger implements ILogger {
	name: string;

	constructor(name: string) {
		this.name = name;
	}

	log(message: string): void {
		console.log("[" + this.name + "] " + message);
	}
}

class RemoteLogger implements ILogger {
	constructor(public name: string) { }

	log(message: string): void {
		console.log("Sending the data to the outside world!");
		console.log("[" + this.name + "] " + message);
	}
}	
	
// abstract ! (TypeScript 1.6)
class LoggerBase implements ILogger {
	constructor(public name: string) { }

	log(message: string): void {
		console.log("LoggerBase");
		console.log("[" + this.name + "] " + message);
	}
}

class ConsoleLogger2 extends LoggerBase {
	constructor(name: string) {
		super(name); // call base class constructor
	}
		
	// override a function!
	log(message: string): void {
		// do your job
		console.log("ConsoleLogger2");
		console.log("[" + this.name + "] " + message);
	}
}
	
// interface do not have get/set (accessors) -> you need to convert the name property to a getName() function if you wanna give that readonly behaviour