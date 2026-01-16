import { Component, inject, OnInit } from '@angular/core';
import { UsuariosService } from '../../../service/usuarios.service';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { Usuario } from '../../../interface/usuario';
import { NavbarPrivateComponent } from "../../../shared/navbar-private/navbar-private.component";
import { CommonModule } from '@angular/common';
import { NavbarAdminComponent } from "../../../shared/navbar-admin/navbar-admin.component";

@Component({
  selector: 'app-perfil-propio-admin',
  standalone: true,
  imports: [NavbarPrivateComponent, RouterLink, CommonModule, NavbarAdminComponent],
  templateUrl: './perfil-propio-admin.component.html',
  styleUrl: './perfil-propio-admin.component.css'
})
export class PerfilPropioAdminComponent implements OnInit{

  ngOnInit(): void {
    this.accederAlosDatos();
  }
  fotoUrl = 'assets/avatar/avatar.png';  // Ruta de la foto de perfil

  usuario?: Usuario;
  id : string | null = null;

  valoracion: number | null = null;       // Valor de la valoración
  stars: number[] = [];
  promedio :number= 0;

  service= inject(UsuariosService);
  ar= inject(ActivatedRoute);
  router = inject(Router);



   // Calcula el promedio de valoraciones
   calcularPromedioValoracion() {
    if (this.usuario?.valoraciones?.length) {
      const suma = this.usuario.valoraciones.reduce((acc, val) => acc + val, 0);
      this.promedio= suma / this.usuario.valoraciones.length;
      console.log('Promedio:'+ this.promedio);
    }
    else
    {
      console.log('Error al carlcular el promedio');
    }

  }

    // Convierte el promedio en estrellas llenas, medias y vacías
    actualizarEstrellas() {
      const promedio = this.promedio / 2; // Promedio en una escala de 5
      const estrellasLlenas = Math.floor(promedio);
      const mediaEstrella = promedio % 1 >= 0.5 ? 1 : 0;
      const estrellasVacias = 5 - estrellasLlenas - mediaEstrella;

      this.stars = [
        ...Array(estrellasLlenas).fill(1),  // Estrellas llenas
        ...Array(mediaEstrella).fill(0.5),  // Media estrella (si aplica)
        ...Array(estrellasVacias).fill(0),  // Estrellas vacías
      ];
    }


  accederAlosDatos()
    {
      this.ar.paramMap.subscribe(
        {
          next: (param)=>
          {
              this.id = param.get('id');
              this.getById()
          },
          error: ()=>{
            alert('Error al acceder a los datos');
          }
        }
      )
    }

    getById()
    {
      this.service.getUsuarioById(this.id).subscribe(
        {
          next: (usuario : Usuario)=>
          {
            this.usuario = usuario;
            this.calcularPromedioValoracion();
            this.actualizarEstrellas(); // Cálculo de estrellas al iniciar
          },
          error: () =>
          {
            alert('Error al acceder a los datos');
          }
        }
    )
    }

    irAModificar(id:string)
    {
      this.router.navigate([`modificar/${id}`]);
    }

   
}
