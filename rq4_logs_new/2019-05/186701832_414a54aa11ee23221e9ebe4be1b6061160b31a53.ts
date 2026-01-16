import { Component, OnInit } from '@angular/core';
import { ReservationVehicule } from '../reservation-vehicule';
import { DataService } from '../service/data.service';

@Component({
  selector: 'app-lire-reservation',
  templateUrl: './lire-reservation.component.html',
  styleUrls:['./lire-reservation.component.css']
})
export class LireReservationComponent implements OnInit {

  showed=false;
  listesReservations:ReservationVehicule[];
  enCours:string="Mes Reservation en cours"
  historique:string="Historique"
  maintenant:Date = new Date(Date.now());

  constructor(private srv:DataService) { }

  ngOnInit() {

    this.srv.afficherLesReservation().subscribe(tab =>this.listesReservations = tab)
  }

  show(){
  this.showed=true;
  }

}