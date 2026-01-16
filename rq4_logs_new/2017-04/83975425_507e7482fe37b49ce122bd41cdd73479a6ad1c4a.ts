import { Injectable, Component, Renderer, ElementRef } from '@angular/core';
import { MdDialog, MdDialogRef } from '@angular/material';
@Injectable()
export class ConfirmDialogService {

  constructor(
    private dialog : MdDialog
  ) { }

  open(text : string){
    let dialogRef = this.dialog.open(ConfirmDialogComponent);
    dialogRef.componentInstance.text = text;
    return dialogRef;
  }

}

@Component({
  template : `
    <h4>{{text}}</h4>
    <button md-button (click)="close(true)">ACEPTAR</button>
    <button md-button (click)="close(false)">CANCELAR</button>
  `
})
export class ConfirmDialogComponent {
  text : string;
  constructor(
    private dialogRef : MdDialogRef<ConfirmDialogComponent>,
  ){}

  close(option){
    this.dialogRef.close(option);
  }
}