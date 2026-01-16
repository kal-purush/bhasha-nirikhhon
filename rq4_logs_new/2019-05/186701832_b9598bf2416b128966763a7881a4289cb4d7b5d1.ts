import { Component, OnInit, Input } from '@angular/core';
import { Collegue } from 'gestion-des-transports-front/src/app/auth/auth.domains';
import { DataService } from '../service/data.service';
import { AuthService } from '../auth/auth.service';
import { Annonce } from '../models/Annonce';

@Component({
  selector: 'app-annonce-liste-covoiturage',
  templateUrl: './annonce-liste-covoiturage.component.html',
  styleUrls: []
})
export class AnnonceListeCovoiturageComponent implements OnInit {

  @Input() private _annonceur: Collegue;
  get annonceur() {
    return this._annonceur;
  }

  set annonceur(annonceur) {
    this._annonceur = annonceur;
  }

  private _annonces: Annonce[];
  get annonces() {
    return this._annonces;
  }

  set annonces(annonces: Annonce[]) {
    this._annonces = annonces;
  }

  private _annoncesEnCours: Annonce[];
  get annoncesEnCours() {
    return this._annoncesEnCours;
  }

  set annoncesEnCours(annoncesEnCours: Annonce[]) {
    this._annoncesEnCours = annoncesEnCours;
  }

  private _annoncesPassees: Annonce[];
  get annoncesPassees() {
    return this._annoncesPassees;
  }

  set annoncesPassees(annoncesPassees: Annonce[]) {
    this._annoncesPassees = annoncesPassees;
  }

  constructor(private _dataService: DataService, private _authService: AuthService) {
    this._annoncesEnCours = new Array<Annonce>();
    this._annoncesPassees = new Array<Annonce>();
  }

  ngOnInit() {
    if (this._annonceur === undefined) {
      this._authService.verifierAuthentification().subscribe(
        colConnecte => this._annonceur = colConnecte
      );
    }
    this._dataService.listeAnnonces(this._annonceur.email).subscribe(
      annonces => this._annonces = annonces,
      error => { },
      () => this.filtreAnnoncesEnCoursEtPassees()
    );
  }

  public filtreAnnoncesEnCoursEtPassees() {
    const DateDuJour = new Date();
    this._annonces.forEach(
      annonce => {
        new Date(annonce.dateTimeDepart) > DateDuJour ? this._annoncesEnCours.push(annonce) : this._annoncesPassees.push(annonce);
      }
    );
    console.log(this._annoncesPassees.length);
  }

}