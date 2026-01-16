import { Component, OnInit, ViewChild } from '@angular/core';
import { Router } from '@angular/router';
import { MyFormComponent } from '../comun/my-form/my-form.component';
import { HttpGralService, apisUrl } from 'src/app/services/http/http.gral.service';
import form_email from 'src/app/forms/form_email';
import form_passOlvidada from 'src/app/forms/form_passOlvidada';
import { AlertService } from 'src/app/services/components/alert.service';


@Component({
  selector: 'app-password-olvidada',
  templateUrl: './password-olvidada.component.html',
  styleUrls: ['./password-olvidada.component.css']
})
export class PasswordOlvidadaComponent implements OnInit {

  formDataTemplateEmail = form_email;
  formDataTemplateNewPass = form_passOlvidada;
  urlResetPassAskCodigo = apisUrl.jugador_ConfirmarPassword;
  urlCambiarPass = apisUrl.jugador_CambiarPasswordOlvidada;

  constructor(
    private router: Router,
    private httpGralService: HttpGralService,
    private alertService: AlertService,

  ) {
  }

  ngOnInit() {
  }

     // del perdir código para cambiar la password
     public submitAskCodigo = (formulario) => {
      this.alertService.success('Codigo solicitado correctamente. En breve estará en su email');
    }
    // para cambiar la password  (codigo + nueva password)
    public submitNewPass = (formulario) => {
      this.alertService.success('Su password ha sido modificada correctamente');
      this.router.navigate(['/']);
    }
}