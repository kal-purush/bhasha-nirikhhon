import { Component } from '@angular/core';
import { NavbarPublicComponent } from "../../../shared/navbar-public/navbar-public.component";
import { LoginAnimationComponent } from "../../../component/LOGIN/login-animation/login-animation.component";

@Component({
  selector: 'app-iniciar-sesion-page',
  standalone: true,
  imports: [NavbarPublicComponent, LoginAnimationComponent],
  templateUrl: './iniciar-sesion-page.component.html',
  styleUrl: './iniciar-sesion-page.component.css'
})
export class IniciarSesionPageComponent {

}