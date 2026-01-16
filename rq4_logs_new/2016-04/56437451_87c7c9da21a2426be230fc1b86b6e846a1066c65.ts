import {Component} from 'angular2/core';

import {IspitRok} from './ispit-rok';
@Component({
    selector: 'ispiti-rok-details',
    template:`
        
         <div style="border-radius: 10px; background: #eee; border: 2px solid #369; padding: 20px; display: inline-block" >
            <span style= "display:inline-block; width:50%;weight:bold">Ispit:</span>
            <span >{{exam.nazivPredmeta}}</span><br>
            Kod Profesora: {{exam.profesor}}<br>
            Datum: {{exam.datum}}<br>
            Ocena: {{exam.polozio}}
            
        </div>
    `,
    inputs:["exam"]
})
export class IspitiRokDetailsComponent{
    public exam: IspitRok;
}