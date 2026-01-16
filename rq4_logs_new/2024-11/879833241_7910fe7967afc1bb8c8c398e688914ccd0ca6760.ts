// login.component.ts
import { Component, inject, OnInit } from '@angular/core';
import { LoginRegisterComponent } from '../login-register/login-register.component';
import { LoginSignInComponent } from '../login-signIn/login-sign-in/login-sign-in.component';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [LoginRegisterComponent, LoginSignInComponent],
  templateUrl: './login-animation.component.html',
  styleUrls: ['./login-animation.component.css']
})
export class LoginAnimationComponent{

  toggleForms() {
    // Seleccionamos el contenedor que contiene los formularios
    const container = document.getElementById('container');

    // Si el contenedor existe, alternamos la clase 'right-panel-active'
    if (container) {
      container.classList.toggle('right-panel-active');
    }
    // Esta clase se puede usar en CSS para aplicar diferentes estilos (por ejemplo, mostrar u ocultar el panel)
  }
}