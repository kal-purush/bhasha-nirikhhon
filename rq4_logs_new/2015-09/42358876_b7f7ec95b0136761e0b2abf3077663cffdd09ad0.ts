import 'zone.js';
import 'reflect-metadata';
import {Component, View, bootstrap} from "angular2/angular2";
import {Greeter} from "./greeter";
import {Inflector} from "common";
@Component({
	selector: "content"
})
@View({
	template: `
		The plural of "dog" is: {{ plural }} <br/> 
		<span>{{ greeter.time }}</span>
	`		
})
class Main {
	public greeter:Greeter;
	public plural:String;
	constructor () {
		this.plural = Inflector.getInstance().pluralize("dog");
		this.greeter = new Greeter();
		this.greeter.start();	
	}
}

bootstrap(Main);