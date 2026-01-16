import { Component, OnInit } from '@angular/core';
import { HttpGralService, apisUrl } from 'src/app/services/http/http.gral.service';
import { AuthenticationService } from 'src/app/services/http/authentication.service';

@Component({
  selector: 'app-detalle-ranking',
  templateUrl: './detalle-ranking.component.html',
  styleUrls: ['./detalle-ranking.component.css']
})
export class DetalleRankingComponent implements OnInit {

  jugadorxranking: any;
  currentUser: any;
  constructor(
    private httpGralService: HttpGralService,
    private authenticationService: AuthenticationService,
  ) {
    this.authenticationService.currentUser.subscribe(x => this.currentUser = x);
   }


   ngOnInit() {
    this.getJugadorEstadisticas();
  }
  getJugadorEstadisticas() {
    this.httpGralService.getDataById(apisUrl.jugadorxranking, this.currentUser.id).subscribe(
      data => {
        this.jugadorxranking = [];
        this.jugadorxranking.push(data);
      });
  }

}