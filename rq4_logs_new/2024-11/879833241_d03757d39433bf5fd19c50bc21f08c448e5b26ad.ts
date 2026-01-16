
import { CommonModule } from '@angular/common';
import { Component, Inject } from '@angular/core';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { MatError, MatFormField, MatLabel } from '@angular/material/form-field';

@Component({
  selector: 'app-calificar-reserva',
  standalone: true,
  imports: [MatFormField,MatLabel,MatError,ReactiveFormsModule,CommonModule],
  templateUrl: './calificar-reserva.component.html',
  styleUrl: './calificar-reserva.component.css'
})
export class CalificarReservaComponent {
  calificacionForm: FormGroup;

  constructor(
    private dialogRef: MatDialogRef<CalificarReservaComponent>,
    private fb: FormBuilder,
    @Inject(MAT_DIALOG_DATA) public data: { reservaId: string }
  ) {
    this.calificacionForm = this.fb.group({
      calificacion: [null, [Validators.required, Validators.min(1), Validators.max(10)]],
    });
  }

  cerrar() {
    this.dialogRef.close();
  }

  enviarCalificacion() {
    if (this.calificacionForm.valid) {
      this.dialogRef.close(this.calificacionForm.value.calificacion);
    }
  }
}