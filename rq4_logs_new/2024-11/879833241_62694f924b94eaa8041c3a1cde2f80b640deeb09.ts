import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { NavbarAdminComponent } from "../../shared/navbar-admin/navbar-admin.component";

@Component({
  selector: 'app-faq-admin',
  standalone: true,
  imports: [CommonModule, NavbarAdminComponent],
  templateUrl: './faq-admin.component.html',
  styleUrls: ['./faq-admin.component.css']
})
export class FaqAdminComponent {
  faqList = [
    { Pregunta: '¿Cómo puedo registrarme?', Respuesta: 'Para registrarte, ve a la página de registro y completa el formulario.', open: false },
    { Pregunta: '¿Cómo restablezco mi contraseña?', Respuesta: 'Puedes restablecer tu contraseña haciendo clic en "Olvidaste tu contraseña" en la página de inicio de sesión.', open: false },
    { Pregunta: '¿Dónde encuentro mis configuraciones de cuenta?', Respuesta: 'Las configuraciones de cuenta están en el menú superior, bajo "Configuración".', open: false }
  ];

  toggleRespuesta(item: any) {
    item.open = !item.open;
  }
}