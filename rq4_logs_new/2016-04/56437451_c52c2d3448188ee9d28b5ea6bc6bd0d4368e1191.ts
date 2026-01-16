import {Component, OnInit} from 'angular2/core';
import {PredmetService} from './predmet.service';
import {Predmet} from './predmet';

@Component({
    selector: 'predmet-list',
    template:`
        <h2>Hello from liste predmeta</h2>
        <ul>
            <li *ngFor = "#predmet of predmeti">
                {{predmet.naziv}} profesor: {{predmet.profesor}}
            </li>
        </ul>
    `,
    providers:[PredmetService]
})
export class PredmetListComponent{
    public predmeti:Predmet[] = [{id: 1, naziv:"vlada", profesor: "vlada"}];
    
    constructor(private _predmetService: PredmetService){}
    
      public getPredmete(){
       
        this._predmetService.getPredmete().subscribe(

      data => { this.predmeti = data},
      
      err => console.error(err),
      
      () => console.log('done loading foods')
    );
    }
    ngOnInit(){
        this.getPredmete();
    }
}