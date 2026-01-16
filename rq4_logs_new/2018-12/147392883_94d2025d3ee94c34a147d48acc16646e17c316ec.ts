import {Component, Inject} from '@angular/core';
import {MAT_DIALOG_DATA, MatDialogRef} from "@angular/material";
import {Review} from "../../shared/model/review";

@Component({
  selector: 'app-review-dialog',
  templateUrl: './review-dialog.component.html',
  styleUrls: ['./review-dialog.component.scss']
})
export class DialogReview{

  public textReview: string;


  constructor(
    public dialogRef: MatDialogRef<DialogReview>,
    @Inject(MAT_DIALOG_DATA) public data: Review) {
    this.textReview = data.review;
  }

  onNoClick(): void {
    this.dialogRef.close();
  }
}