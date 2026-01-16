import { Component, OnInit } from '@angular/core';
import { AuthService } from '../auth/auth.service';
import { Collegue } from '../auth/auth.domains';
import { Observable } from 'rxjs';

@Component({
  selector: 'app-accueil-administrateur',
  templateUrl: './accueil-administrateur.component.html'
})
export class AccueilAdministrateurComponent implements OnInit {

  administrateur:Observable<Collegue>;
  constructor(private _authSrv:AuthService) { }

  ngOnInit() {

    this.administrateur = this._authSrv.collegueConnecteObs;

  }

}