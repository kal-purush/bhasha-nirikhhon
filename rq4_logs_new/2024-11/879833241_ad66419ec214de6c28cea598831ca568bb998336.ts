import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { RouterLink } from '@angular/router';
import { NavbarPrivateComponent } from '../../../shared/navbar-private/navbar-private.component';

@Component({
  selector: 'app-nosotros',
  standalone: true,
  imports: [CommonModule, NavbarPrivateComponent,RouterLink],
  templateUrl: './nosotros.component.html',
  styleUrl: './nosotros.component.css'
})
export class NosotrosComponent {
  avatarMujer: string = 'assets/avatar/mujer.png';
  avatarHombre: string = 'assets/avatar/hombre.png';

}