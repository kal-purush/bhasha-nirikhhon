class Tool {
	private name: string = "Hammer";
	
	doSomething(): void {
		// I expect 'this,name' to always have the value 'Hammer' right?
		console.log("[" + this.name + "] Hammering. this is my call context", this);
	}
}

class User {
	private name: string = "user";
	
	private tool: Tool; // let's do some composition
	
	public doSomething: () => void; // and expose to the outside world the functions from our component class
	
	constructor(tool: Tool) {
		this.tool = tool;
		this.doSomething = this.tool.doSomething; // you are messing up with the 'this'!
		
	}	
}

var u = new User(new Tool());
u.doSomething();
































// you probably would like to do:
// this.doSomething2 = () => { this.tool.doSomething(); } // this is what you really wanna do!
	