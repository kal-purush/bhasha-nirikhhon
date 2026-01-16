class HelloWorld {
	
	constructor(public message: string) {
	}
}

var hello = new HelloWorld('Hi');
console.log(hello.message);