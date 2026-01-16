import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { MatDialogRef } from '@angular/material/dialog';

@Component({
  selector: 'app-login-continuar-registro',
  standalone: true,
  imports: [CommonModule, ReactiveFormsModule],
  templateUrl: './login-continuar-registro.component.html',
  styleUrls: ['./login-continuar-registro.component.css']
})
export class LoginContinuarRegistroComponent {
  signUpForm2!: FormGroup;

  constructor(
    private fb: FormBuilder,
    private dialogRef: MatDialogRef<LoginContinuarRegistroComponent>
  ) {
    this.signUpForm2 = this.fb.group({
      profesion: ['', Validators.required],
      disponibilidad: ['', Validators.required],
      descripcion: ['', [Validators.required, Validators.maxLength(250)]],
      zona: ['', Validators.required],
      telefono: ['', Validators.required]
    });
  }

  closeDialog(): void {
    this.dialogRef.close();
  }

  onSubmit(): void {
    if (this.signUpForm2.valid) {
      this.dialogRef.close(this.signUpForm2.value); // Envía los datos y cierra el diálogo
    }
  }
}