import { Component, OnInit } from '@angular/core';
import { AuthenticationService } from 'src/app/services/http/authentication.service';
import { apisUrl, HttpGralService } from 'src/app/services/http/http.gral.service';

@Component({
  selector: 'app-detalle-partidos',
  templateUrl: './detalle-partidos.component.html',
  styleUrls: ['./detalle-partidos.component.css']
})
export class DetallePartidosComponent implements OnInit {

  jugadorxpartidos: any;
  currentUser: any;
  constructor(
    private httpGralService: HttpGralService,
    private authenticationService: AuthenticationService,
  ) {
    this.authenticationService.currentUser.subscribe(x => this.currentUser = x);
  }

  ngOnInit() {
    this.getJugadorPartidos();
  }
  getJugadorPartidos() {
    this.httpGralService.getDatas(apisUrl.jugadorResumenPartidos).subscribe(
      data => {
        this.jugadorxpartidos = data;
      });
  }

}