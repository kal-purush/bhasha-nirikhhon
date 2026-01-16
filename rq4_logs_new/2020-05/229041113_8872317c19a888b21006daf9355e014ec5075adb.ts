import { Component, OnInit } from '@angular/core';
import { HttpGralService, apisUrl } from 'src/app/services/http/http.gral.service';

@Component({
  selector: 'app-jugador-ranking',
  templateUrl: './jugador-ranking.component.html',
  styleUrls: ['./jugador-ranking.component.css']
})
export class JugadorRankingComponent implements OnInit {

  jugadoresxranking: any;

  constructor(private httpGralService: HttpGralService) { }

  ngOnInit() {
    this.getJugadorxResultado();
  }

  getJugadorxResultado() {
    this.httpGralService.getDatas(apisUrl.jugadorxranking).subscribe(
      jxr => {
        this.jugadoresxranking = jxr;
      });
  }

}