import { Component, inject } from '@angular/core';
import { NavbarAdminComponent } from "../../shared/navbar-admin/navbar-admin.component";
import { SearchBarComponent } from "../Inicio/search-bar/search-bar.component";
import { CommonModule } from '@angular/common';
import { Usuario } from '../../interface/usuario';
import { Router } from '@angular/router';
import { UsuariosService } from '../../service/usuarios.service';

@Component({
  selector: 'app-home-admin',
  standalone: true,
  imports: [NavbarAdminComponent, SearchBarComponent,CommonModule],
  templateUrl: './home-admin.component.html',
  styleUrl: './home-admin.component.css'
})
export class HomeAdminComponent {
  ngOnInit(): void {
    this.accederAusuarios()
  }

  listaUsuarios : Usuario[] =[];

  service = inject(UsuariosService);
  router = inject(Router);
  stars: number[] = [];


  // Calcula el promedio de valoraciones
  calcularPromedioValoracion(valoraciones: number[] | undefined): number {
    let promedio = 0;

  if (Array.isArray(valoraciones) && valoraciones.length > 0) {
    const suma = valoraciones.reduce((acc, val) => acc + val, 0);
    promedio = suma / valoraciones.length;
  }

  // Actualizamos las estrellas con el promedio calculado (o 0 si no hay valoraciones)
  this.actualizarEstrellas(promedio);

  return promedio;
  }

  actualizarEstrellas(promedio : number) {

    if (promedio === 0) {
      // Si el promedio es 0, muestra 5 estrellas vacías
      this.stars = [0, 0, 0, 0, 0];
      return;
    }

    const promedioDividio = promedio / 2; // Promedio en una escala de 5
    const estrellasLlenas = Math.floor(promedioDividio);
    const mediaEstrella = promedioDividio % 1 >= 0.5 ? 1 : 0;
    const estrellasVacias = 5 - estrellasLlenas - mediaEstrella;

    this.stars = [
      ...Array(estrellasLlenas).fill(1),  // Estrellas llenas
      ...Array(mediaEstrella).fill(0.5),  // Media estrella (si aplica)
      ...Array(estrellasVacias).fill(0),  // Estrellas vacías
    ];
  }


  accederAusuarios()
  {
      this.service.getUsuarios().subscribe(
        {
          next: (usuarios: Usuario[])=>{
                this.listaUsuarios = usuarios;
          },
          error: (e: Error)=>{
            alert('Ha ocurrido un error:  ' + e.message);
          }
        }
      )
  }


  irADetalles(id:string)
  {
    this.router.navigate([`perfil-trabajador/${id}`]);
  }
  
}