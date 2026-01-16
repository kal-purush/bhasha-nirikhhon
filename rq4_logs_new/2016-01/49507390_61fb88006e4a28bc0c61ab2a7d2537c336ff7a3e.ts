import {Component} from "angular2/core";
import {bootstrap} from "angular2/platform/browser";

@Component({
	selector:"ez-app",
	template:`	
		<h1>管道级联</h1>
		<p>{{ (text | slice:0:20 | uppercase) + "..." }}</p>
		<p>{{ (word | slice:0:1 | uppercase) + (word | slice:1:5) + (word | slice:5:6 | uppercase) + (word | slice:6)}}</p>
	`
})
class EzApp{
	constructor(){
		this.text = `Next we demonstrate this pipe in a test component 
        			whose template defines two bindings`;
      this.word = `stay foolish`;
	}
}
		
bootstrap(EzApp);