import {Component, Inject, OnInit} from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA} from '@angular/material/dialog';
import {IrescheduleService, StatusComponent} from '../status.component';

@Component({
  selector: 'app-reschedule-popup',
  templateUrl: './reschedule-popup.component.html',
  styleUrls: ['./reschedule-popup.component.css']
})
export class ReschedulePopupComponent {

  constructor(
    public dialogRef: MatDialogRef<StatusComponent>,
    @Inject(MAT_DIALOG_DATA) public data: IrescheduleService) {}

  onNoClick(): void {
    this.dialogRef.close();
  }

}