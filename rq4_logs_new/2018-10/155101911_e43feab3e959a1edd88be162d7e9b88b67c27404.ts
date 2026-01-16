import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';

@Component({
  selector: 'message-dialog',
  templateUrl: './message-dialog.component.html',
  styleUrls: ['./message-dialog.component.scss']
})
export class MessageDialogComponent {

  constructor(
    private _dialogRef: MatDialogRef<MessageDialogComponent>
    , @Inject(MAT_DIALOG_DATA) public data: any
  ) { }
}