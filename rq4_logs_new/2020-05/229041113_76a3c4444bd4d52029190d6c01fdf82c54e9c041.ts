import { Injectable } from '@angular/core';
import { HttpGralService, apisUrl } from '../http/http.gral.service';
import { Observable } from 'rxjs';

@Injectable()
export class CombosService {

  perfil = 'perfil';
  posicion = 'posicion';
  jugadorestado = 'jugadorestado';
  partidoestado = 'partidoestado';

  combos = {};
  constructor( private httpGralService: HttpGralService, ) { }

  getCombo( combo ) {
    return new Observable<any>(observer => {
      if (this.combos[combo]) {
        observer.next(this.combos[combo]);
      } else {
        this.httpGralService.getDatas(`${apisUrl.codigos}/${combo}` ).subscribe(
          data => {
            this.combos[combo] = data;
            observer.next(this.combos[combo]);
          });
      }
    });
  }
}