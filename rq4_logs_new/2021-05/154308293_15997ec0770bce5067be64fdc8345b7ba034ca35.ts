import {
  ChangeDetectionStrategy,
  Component,
  Inject,
} from '@angular/core';
import {
} from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';


@Component({
  templateUrl: './terms-field-dialog.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class TermsFieldDialogComponent {

  public content;

  constructor(
    public dialogRef: MatDialogRef<TermsFieldDialogComponent>,
    @Inject(MAT_DIALOG_DATA) public data,
  ) {
    this.content = data.content;
  }

}