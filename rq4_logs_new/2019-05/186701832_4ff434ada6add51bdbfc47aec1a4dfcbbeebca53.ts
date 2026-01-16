import { Component, OnInit } from '@angular/core';
import { DataService } from '../service/data.service';
import { InfoVehicule } from '../info-vehicule';

@Component({
  selector: 'app-creation-reservation',
  templateUrl: './creation-reservation.component.html',
  styleUrls: ['./creation-reservation.component.css']
})
export class CreationReservationComponent implements OnInit {

  constructor(private _srv:DataService) { }

numbers:number[] = new Array();
 minutes:number[] = new Array();

 listeVehicules:InfoVehicule[];

  ngOnInit() {


  for (let i = 0; i < 24 ; i++) {
    this.numbers.push(i);
  }

  for(let i=0; i<60; i++){
    this.minutes.push(i);
  }

this._srv.afficherInfo().subscribe(tab=>this.listeVehicules = tab);

  //récupérer les données dès le démarrage



  }

}