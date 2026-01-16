import { Component, OnInit } from '@angular/core';
import { Annonce } from "../models/Annonce";
import { DataService } from '../service/data.service';

@Component({
  selector: 'app-reservation-covoiturage',
  templateUrl: './reservation-covoiturage.component.html',
  styleUrls: ['./reservation-covoiturage.component.scss']
})
export class ReservationCovoiturageComponent implements OnInit {

  private _annonces: Annonce[];

  get annonces() {
    return this._annonces;
  }

  set annonces(annonces: Annonce[]) {
    this._annonces = annonces;
  }

  constructor(private _dataService: DataService) { }

  ngOnInit() {
    if (this._annonces === undefined) {
      this._dataService.listeToutesAnnoncesEnCours().subscribe(
        annoncesEnCours => this._annonces = annoncesEnCours,
        err => {},
        () => {this._annonces.forEach(a => console.log("je vois " + a.annonceurEmail + " " + a.dateTimeDepart))}
      )
    }
  }

}