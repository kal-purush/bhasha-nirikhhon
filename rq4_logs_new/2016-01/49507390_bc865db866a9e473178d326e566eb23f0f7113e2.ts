import {Component} from "angular2/core";
import {bootstrap} from "angular2/platform/browser";

@Component({
	selector:"ez-app",
	template:`	
    	<h1 [ngClass]="cns">EZ-APP</h1>
	`,
    styles : [`
    	.colorful{color:red;}
        .italic{font-style:italic}
		    .size{font-size: 28px; color:blue}
        .bold{font-weight:bold}
    `]
})
class EzApp{
	constructor(){
    	this.cns = {
        	colorful : false,
            italic : true,
          	size: true,
            bold : true
        };
    }
}
		
bootstrap(EzApp);