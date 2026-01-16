import { Component, inject, Injectable } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import {
  MatDialog,
  MatDialogModule,
  MatDialogRef,
} from '@angular/material/dialog';
import { Observable } from 'rxjs';

@Component({
  selector: 'app-comfirmation-dialog',
  template: `
    <h2 mat-dialog-title>Deletar pensamento</h2>
    <mat-dialog-content>
      Deseja mesmo deletar este pensamento?
    </mat-dialog-content>
    <mat-dialog-actions align="end">
      <button mat-button (click)="onNot()">Não</button>
      <button mat-button (click)="onYes()" cdkFocusInitial>Sim</button>
    </mat-dialog-actions>
  `,
  standalone: true,
  imports: [MatButtonModule, MatDialogModule],
})
export class ConfirmationDialogComponent {
  matDialogRef = inject(MatDialogRef);
  onNot() {
    this.matDialogRef.close(false);
  }
  onYes() {
    this.matDialogRef.close(true);
  }
}

@Injectable({
  providedIn: 'root',
})
export class ConfirmationDialogService {
  matDailog = inject(MatDialog);
  constructor() {}
  openDialog(): Observable<boolean> {
    return this.matDailog.open(ConfirmationDialogComponent).afterClosed();
  }
}