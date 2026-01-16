import { Component, OnInit } from '@angular/core';
import { MatDialogRef } from '@angular/material';

@Component({
  selector: 'reject-confirmation-dialog',
  templateUrl: './reject-confirmation-dialog.component.html',
  styleUrls: ['./reject-confirmation-dialog.component.scss']
})
export class RejectConfirmDialogComponent implements OnInit {

  constructor(private dialogRef: MatDialogRef<RejectConfirmDialogComponent>) { }

  ngOnInit() {
  }

}