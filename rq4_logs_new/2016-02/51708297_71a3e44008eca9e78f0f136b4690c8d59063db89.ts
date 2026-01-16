import 'rxjs/Rx';

import {bootstrap} from 'angular2/platform/browser';
import {Subject} from "rxjs/Subject";
import {Observable} from "rxjs/Observable";
import {Observer} from "rxjs/Observer";
import {Component} from 'angular2/core';


@Component({
	selector: 'my-observer',
	template: `
            <div class="container-fluid">
        <div class="row">
        <div class="col-xs-12">
        <h4>Observing the interval</h4>
        </div>
        </div>
        <div class="row">
        <div class="col-xs-4">
        (dont forget to share....)
        </div>
        <div class="col-xs-4">
      <label>Observer 1: </label> {{observer1}}
      </div>
      <div class="col-xs-4">
      <label>Observer 2: </label>  {{observer2}}      
   </div>
   </div>

    <div class="row">
  <button type="button" class="btn btn-primary" (click)=gogoGadget()>Go go gadget</button>
    <button type="button" class="btn btn-primary" (click)=observe1()>Observe nr 1</button>
      <button type="button" class="btn btn-primary" (click)=observe2()>Observe nr 2</button>
      </div>
      <div class="row somemarging">
      <p>Example stolen and mutilated from :</p> 
      <a href="http://blog.jhades.org/functional-reactive-programming-for-angular-2-developers-rxjs-and-observables/">
      http://blog.jhades.org/functional-reactive-programming-for-angular-2-developers-rxjs-and-observables/</a>
      
      </div>
      </div>
	 `,
	styles: [`.somemarging {margin-top:20px;}`]
})


export class observeComponent {
    isShared:boolean;
    observer1:number;
	observer2:number;
	
	obs: Observable<number>;	


	constructor() {
	   this.observer1 = NaN;
	   this.observer2 = -1;	
	}
	
	gogoGadget() {
        console.log("doing the gogogogo");
        
		    this.obs = Observable.interval(30)
		//	.take(4)
			.do(i  => console.log(  i.toString(10) + " being emitted") )
		//	.share()
			;
	}
    observe1() {
			this.obs.subscribe(value =>this.observer1 = value);
    }
    
    observe2() {
			this.obs.subscribe(value => this.observer2 = value);
    }
}