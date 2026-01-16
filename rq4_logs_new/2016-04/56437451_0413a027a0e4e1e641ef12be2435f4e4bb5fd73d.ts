import {Component,OnInit} from 'angular2/core';
import {Term} from './term';
import {Router, RouteParams} from 'angular2/router';

import {IspitiRokComponent} from './ispiti-rok.component';


@Component({
    selector:'future-terms-details',
    templateUrl:'res/html/term/future-terms-details.component.html',
    directives:[IspitiRokComponent]
})
export class FutureTermsDetailsComponent implements OnInit{
   // public term: Term;
   datumPocetka:string;
   datumZavrsetka:string;
   naziv:string;
   id: number;
   
   //temp:Date;

    constructor(private _router:Router, private _routeParams: RouteParams){}
  
ngOnInit():any{
        
        this.datumPocetka = this._routeParams.get("datumPocetka");
        this.datumZavrsetka = this._routeParams.get("datumZavrsetka");
        this.id = +this._routeParams.get("id");
        this.naziv = this._routeParams.get("naziv");
    }
    onBackToTermList(){
        this._router.navigate(['FutureTerms']);
    }
}