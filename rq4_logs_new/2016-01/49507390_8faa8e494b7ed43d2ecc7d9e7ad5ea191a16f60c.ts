import {Component,Pipe} from "angular2/core";
import {bootstrap} from "angular2/platform/browser";
import {Observable} from "rxjs/Observable";

@Pipe({name: "countDown"})
class CountDown{
	transform(input){
    	let counter = input;
      	return new Observable(o => {
        	setInterval(_ => {
            	o.next(counter);
              	counter--;
              	if(counter < 0) {
              		o.complete();
              	}
            }, 1000);
        })
    }
}

@Component({
	selector:"ez-app",
	template:`	
		<h1>AsyncPipe</h1>
		<p>{{ data | async | json }}</p>
		<p>{{ num | countDown | async}}</p>
	`,
  	pipes: [CountDown]
  	
})

class EzApp{
	constructor(){
		this.data = window.fetch("api/music.json")
        	.then( rsp => rsp.json());
      
      	this.num = 100;
	}
}
		
bootstrap(EzApp);