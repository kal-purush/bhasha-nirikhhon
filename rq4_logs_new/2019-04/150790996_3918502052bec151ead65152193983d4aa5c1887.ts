import { Component, OnInit, Output, EventEmitter } from '@angular/core';
import { Guia } from 'src/model/guia.model';
import { FormGroup } from '@angular/forms';
import { Subscription } from 'rxjs';
import { GuiaService } from 'src/app/shared/guia.service';
import { NotifierService } from 'angular-notifier';
import { BsModalRef } from 'ngx-bootstrap/modal';

@Component({
  selector: 'app-eliminar-guiabloque',
  templateUrl: './eliminar-guiabloque.component.html',
  styleUrls: ['./eliminar-guiabloque.component.css']
})
export class EliminarGuiabloqueComponent implements OnInit {

  constructor(
    private guiaService: GuiaService,
    private notifier: NotifierService,
    private bsModalRef: BsModalRef
  ) { }

  @Output() eliminarGuiaBloqueEvent = new EventEmitter();

  guia: Guia;
  guias: Guia[] = [];
  eliminarForm: FormGroup;

  eliminarGuiaBloqueSubscription: Subscription;

  ngOnInit() {
    this.eliminarForm = new FormGroup({})
  }

  onSubmit(form: any){
    let numero = Number(this.guia.numeroGuia)
      this.eliminarGuiaBloqueSubscription = this.guiaService.eliminarGuia(numero).subscribe(
        guia => {
          this.notifier.notify('success', 'Se ha modificado la guía correctamente');
          this.bsModalRef.hide();
          this.eliminarGuiaBloqueEvent.emit(guia);
        },
        // error => {
        //   this.notifier.notify('error', 'El número modificado ya existe');
        // }
      );
  }

}