import { Component, OnInit, Input, ViewEncapsulation } from '@angular/core';
import { HttpGralService, apisUrl } from 'src/app/services/http/http.gral.service';
import { ConfirmationService } from 'primeng/api';
import { AlertService } from 'src/app/services/components/alert.service';

@Component({
  selector: 'app-gestion-pistas',
  templateUrl: './gestion-pistas.component.html',
  styleUrls: ['./gestion-pistas.component.css'],
  encapsulation: ViewEncapsulation.None
})
export class GestionPistasComponent implements OnInit {

  @Input() idpartido: any;
  @Input() currentUser: any;
  @Input() idpartido_estado: any;
  @Input() partido: any;

  partidoxpista = [];
  responsiveOptions;
  constructor(
    private httpGralService: HttpGralService,
    private confirmationService: ConfirmationService,
    private alertService: AlertService,
  ) {
    this.responsiveOptions = [
      {
          breakpoint: '1024px',
          numVisible: 3,
          numScroll: 3
      },
      {
          breakpoint: '768px',
          numVisible: 2,
          numScroll: 2
      },
      {
          breakpoint: '560px',
          numVisible: 1,
          numScroll: 1
      }
  ];
  }

  ngOnInit() {
    this.getPartidoxPista();
  }

  getPartidoxPista() {
    this.httpGralService.getDataById(apisUrl.partidosxpistas, this.idpartido).subscribe(
      pxp => {
        this.partidoxpista = pxp;
      });
  }

  setMarcador(a,b){}
}