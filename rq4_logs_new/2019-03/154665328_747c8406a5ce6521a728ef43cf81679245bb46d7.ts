import { Component, OnInit, Inject } from '@angular/core';
import { MatDialogRef } from '@angular/material';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';

@Component({
  selector: 'kick-dialog',
  templateUrl: './kick-dialog.component.html',
  styleUrls: ['./kick-dialog.component.scss']
})
export class KickDialogComponent implements OnInit {

  public name: string;

  constructor(@Inject(MAT_DIALOG_DATA) private data: { displayName: string },
  private dialogRef: MatDialogRef<KickDialogComponent>) {}

  ngOnInit() {
    this.name = this.data.displayName;
  }
}