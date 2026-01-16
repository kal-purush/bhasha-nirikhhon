import {Component,OnInit} from 'angular2/core';
import {Term} from './term';
import {Router, RouteParams} from 'angular2/router';
import {ExamListComponent} from '../exam/exam-list.component';


@Component({
    selector:'term-details',
    template: `
        <h2>Details for term from {{datum}}</h2> 
        <exam-list><exam-list>
        <button (click)= "onBackToTermList()">Back to Term list</button>
    `,
    directives:[ExamListComponent]
})
export class TermDetailsComponent implements OnInit{
   // public term: Term;
   datum:string;
   pass:boolean;
   temp:Date;

    constructor(private _router:Router, private _routeParams: RouteParams){}
  
ngOnInit():any{
        //this.pass = this._routeParams.get("pass");
        //this.term = {date : this._routeParams.get("date"),pass : this._routeParams.get("pass")};
        this.datum = this._routeParams.get("date");
        this.datum = this.datum.substring(0,10);
        //pass = this._routeParams.get("pass");
        
    }
    onBackToTermList(){
        //this._router.navigate(['Terms']);
    }
}