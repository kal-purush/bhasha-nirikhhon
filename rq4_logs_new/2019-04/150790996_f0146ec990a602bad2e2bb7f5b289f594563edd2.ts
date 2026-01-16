import { Component, OnInit, Output, EventEmitter } from '@angular/core';
import { TipoServicioService } from 'src/app/shared/tiposervicio.service';
import { BsModalRef } from 'ngx-bootstrap/modal';
import { NotifierService } from 'angular-notifier';
import { TipoServicio } from 'src/model/tiposervicio.model';
import { FormGroup, FormControl, Validators } from '@angular/forms';
import { Subscription } from 'rxjs';

@Component({
  selector: 'app-agregar-tiposervicio',
  templateUrl: './agregar-tiposervicio.component.html',
  styleUrls: ['./agregar-tiposervicio.component.css']
})
export class AgregarTipoServicioComponent implements OnInit {

  constructor(
    private tipoServicioService: TipoServicioService,
    private bsModalRef: BsModalRef,
    private notifier: NotifierService,
  ) { }

  @Output() tipoServicioCreadoEvent = new EventEmitter<TipoServicio>();

  servicio: TipoServicio;
  servicios: TipoServicio[]  = [];
  agregarForm: FormGroup;

  crearTipoServicioSubscription: Subscription;

  ngOnInit() {
    this.agregarForm = new FormGroup({
      'nombre' : new FormControl('', Validators.required),
    })
  }

  onSubmit(servicio) {
    if (this.agregarForm.controls['nombre'].value.length !== 0) {
      this.crearTipoServicioSubscription = this.tipoServicioService.agregarTipoServicio(servicio).subscribe(
        plazo => {
          this.notifier.notify('success', 'SE AGREGÓ EL TIPO DE SERVICIO CON ÉXITO');
          this.bsModalRef.hide();
          this.tipoServicioCreadoEvent.emit(servicio);
        },
        error => {
          this.notifier.notify('error', error.error.mensaje);
        }
      );
    }
    else {
      this.notifier.notify('error', 'DEBE INGRESAR EL NOMBRE');
    }
  }

}