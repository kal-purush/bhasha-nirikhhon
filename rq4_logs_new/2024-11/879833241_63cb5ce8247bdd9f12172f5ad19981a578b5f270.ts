import { Component } from '@angular/core';
import { LoginRegisterComponent } from "../../login-register/login-register.component";
import { OlvidasteContraseniaComponent } from '../../olvidaste-contrasenia/olvidaste-contrasenia.component';
import { RouterModule } from '@angular/router';

@Component({
  selector: 'app-login-page',
  standalone: true,
  imports: [
    LoginRegisterComponent,
    OlvidasteContraseniaComponent,
    RouterModule
  ],
  templateUrl: './login-page.component.html',
  styleUrl: './login-page.component.css'
})
export class LoginPageComponent {

}