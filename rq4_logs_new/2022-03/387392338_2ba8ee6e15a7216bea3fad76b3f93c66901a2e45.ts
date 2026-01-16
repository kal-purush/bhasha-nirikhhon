import { Component, Inject } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from "@angular/material/dialog";


@Component({
  selector: 'de-annotation-note-dialog',
  templateUrl: './annotation-note-dialog.component.html',
  styleUrls: ['./annotation-note-dialog.component.css']
})

export class AnnotationNoteDialogComponent {

  constructor(@Inject(MAT_DIALOG_DATA) public note: string, 
  private dialogRef: MatDialogRef<AnnotationNoteDialogComponent>) { }
  
  public submitNote(): void {
    if (!this.isValidInput()) return;
    this.dialogRef.close(this.note);
  }

  private isValidInput(): boolean {
    return this.note != '';
  }

  public close(): void {
    this.dialogRef.close();
  }
}