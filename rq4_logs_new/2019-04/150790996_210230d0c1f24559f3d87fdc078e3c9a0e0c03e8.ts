import { Component, OnInit, Output, EventEmitter } from '@angular/core';
import { BsModalRef } from 'ngx-bootstrap/modal';
import { NotifierService } from 'angular-notifier';
import { ClasificacionService } from 'src/app/shared/clasificacion.service';
import { Clasificacion } from 'src/model/clasificacion.model';
import { FormGroup, FormControl, Validators } from '@angular/forms';
import { Subscription } from 'rxjs';

@Component({
  selector: 'app-agregar-clasificacion',
  templateUrl: './agregar-clasificacion.component.html',
  styleUrls: ['./agregar-clasificacion.component.css']
})
export class AgregarClasificacionComponent implements OnInit {

  constructor(
    private bsModalRef: BsModalRef,
    private notifier: NotifierService,
    private clasificacionService: ClasificacionService
  ) { }

  @Output() clasificacionCreadaEvente = new EventEmitter<Clasificacion>();

  clasificacion: Clasificacion;
  clasificaciones: Clasificacion[] = [];
  agregarForm: FormGroup;

  crearClasifiacionSubscription: Subscription;

  ngOnInit() {
    this.agregarForm = new FormGroup({
      'nombre' : new FormControl('', Validators.required)
    })
  }

  onSubmit(clasificacion){
    if (this.agregarForm.controls['nombre'].value.length !== 0) {
      this.crearClasifiacionSubscription = this.clasificacionService.agregarClasificacion(clasificacion).subscribe(
        clasificacion => {
          this.notifier.notify('success', 'SE AGREGÓ LA CLASIFICACIOÓN CON ÉXITO');
          this.bsModalRef.hide();
          this.clasificacionCreadaEvente.emit(clasificacion);
        },
        error => {
          this.notifier.notify('error', error.error.mensaje);
        }
      );
    }
    else {
      this.notifier.notify('error', 'DEBE INGRESAR EL NOMBRE DE LA CLASIFICACIÓN');
    }
  }

}