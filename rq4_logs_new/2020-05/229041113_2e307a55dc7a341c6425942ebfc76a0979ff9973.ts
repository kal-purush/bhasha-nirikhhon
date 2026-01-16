import { Component, OnInit } from '@angular/core';
import { HttpGralService, apisUrl } from 'src/app/services/http/http.gral.service';
import { AuthenticationService } from 'src/app/services/http/authentication.service';

@Component({
  selector: 'app-detalle-pareja',
  templateUrl: './detalle-pareja.component.html',
  styleUrls: ['./detalle-pareja.component.css']
})
export class DetalleParejaComponent implements OnInit {

  jugadorxpareja: any;
  currentUser: any;
  constructor(
    private httpGralService: HttpGralService,
    private authenticationService: AuthenticationService,
  ) {
    this.authenticationService.currentUser.subscribe(x => this.currentUser = x);
   }

   ngOnInit() {
    this.getJugadorResumenParejas();
  }
  getJugadorResumenParejas() {
    this.httpGralService.getDatas(apisUrl.jugadorResumenParejas).subscribe(
      data => {
        this.jugadorxpareja = data;
      });
  }

}