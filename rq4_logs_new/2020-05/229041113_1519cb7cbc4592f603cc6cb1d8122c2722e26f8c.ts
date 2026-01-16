import { Component, OnInit, Input } from '@angular/core';
import { HttpGralService, apisUrl } from 'src/app/services/http/http.gral.service';

@Component({
  selector: 'app-gestion-resultado',
  templateUrl: './gestion-resultado.component.html',
  styleUrls: ['./gestion-resultado.component.css']
})
export class GestionResultadoComponent implements OnInit {

  @Input() currentPartido: any;
  jugadoresxresultado: [];
  constructor( private httpGralService: HttpGralService, ) { }

  ngOnInit() {
    this.currentPartido.subscribe(partido => {

      if (partido && partido.finalizado) {
        this.getJugadorxRanking(partido.id);
      }


    });
  }
  getJugadorxRanking(idpartido) {
    this.httpGralService.getDataById(apisUrl.partidoxpistaxranking, idpartido).subscribe(
      pxp => {
        this.jugadoresxresultado = pxp;
      });
  }

}