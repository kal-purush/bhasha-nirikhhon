import {Component, OnInit} from "@angular/core";
import {Input} from "@angular/core";
import {Reclamo} from "../model/reclamo";

@Component({
  selector: 'reverse-checkbox-luminaria',
  template: `<div *ngIf="reclamo?.tipoDeReclamo?.problemaPalo">Hay un problema en el palo</div>
             <div *ngIf="reclamo?.tipoDeReclamo?.problemaFoco">Hay un problema en el foco</div>`

})

export class ReverseCheckboxLuminariaComponent{
  @Input() reclamo: Reclamo;

}


@Component({
  selector: 'reverse-checkbox-arboleda',
  template: `<div *ngIf="reclamo?.tipoDeReclamo?.problemaPoda">Necesita podar su arbol</div>
             <div *ngIf="reclamo?.tipoDeReclamo?.problemaCaida">Su arbol esta caido</div>`

})

export class ReverseCheckboxArboledaComponent{
  @Input() reclamo: Reclamo;

}

@Component({
  selector: 'reverse-checkbox-bacheo',
  template: ' Existe un bache en esta direccion '

})

export class ReverseCheckboxBacheoComponent{
  @Input() reclamo: Reclamo;

}

@Component({
  selector: 'reverse-checkbox-semaforo',
  template: `<div *ngIf="reclamo?.tipoDeReclamo?.problemaEstructura">El semaforo tiene algun problema en la estructura</div>
             <div *ngIf="reclamo?.tipoDeReclamo?.problemaMalFuncionamiento">El semaforo funciona de forma incorrectaa</div>
             <div *ngIf="reclamo?.tipoDeReclamo?.problemaNoFunciona">El semaforo no funciona</div>`

})

export class ReverseCheckboxSemaforoComponent{
  @Input() reclamo: Reclamo;

}

export const reverseCheckboxComponent =
  [ ReverseCheckboxArboledaComponent, ReverseCheckboxLuminariaComponent,
    ReverseCheckboxBacheoComponent, ReverseCheckboxSemaforoComponent];