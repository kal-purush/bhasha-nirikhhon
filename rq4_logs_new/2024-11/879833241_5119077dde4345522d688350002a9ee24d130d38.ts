import { Component, Inject, inject, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { UsuariosService } from '../../../service/usuarios.service';
import { Usuario } from '../../../interface/usuario';

import { NavbarPrivateComponent } from '../../../shared/navbar-private/navbar-private.component';
import { CommonModule } from '@angular/common';
import { NavbarAdminComponent } from '../../../shared/navbar-admin/navbar-admin.component';
import { AuthService } from '../../../service/auth.service';


@Component({
  selector: 'app-modificar-perfiles-admin',
  standalone: true,

  imports: [NavbarAdminComponent, ReactiveFormsModule,RouterLink, CommonModule],

  templateUrl: './modificar-perfiles-admin.component.html',
  styleUrl: './modificar-perfiles-admin.component.css'
})
export class ModificarPerfilesAdminComponent implements OnInit {

  ngOnInit(): void {
    this.rellenarDatosEnFormulario();
  }

  id : string | null = null;
  activatedRoute = inject(ActivatedRoute);
  fotoUrl = 'assets/avatar/avatar.png';

  fb= inject(FormBuilder);
  service= inject(UsuariosService);
  router= inject(Router);
  usuario?: Usuario;


  formulario = this.fb.nonNullable.group(
      {
        nombre: ['', [Validators.required]],
        profesion: [''],
        disponibilidad: [''],
        zona: [''],
        descripcion: [''],
        telefono:['',[Validators.required] ],
        email:[this.usuario?.email!],
        password:[this.usuario?.password!],
        rol:[this.usuario?.rol!]
      }
    )

   rellenarDatosEnFormulario()
    {
      this.activatedRoute.paramMap.subscribe(
        {
          next: (param)=>
          {
            this.id = param.get('id');
            this.getByid(this.id);

          },
          error: (e : Error)=>{
            console.log('Error al recibir los datos:' + e.message );
          }
        }
      )
    }

    getByid(id: string| null){
      this.service.getUsuarioById(id).subscribe(
        {
          next: (usuario : Usuario)=>
          {
            this.usuario = usuario;
                this.formulario.controls['nombre'].setValue(usuario.nombre);
                this.formulario.controls['profesion'].setValue(usuario.profesion!);
                this.formulario.controls['disponibilidad'].setValue(usuario.disponibilidad!);
                this.formulario.controls['zona'].setValue(usuario.zona!);
                this.formulario.controls['descripcion'].setValue(usuario.descripcion!);
                this.formulario.controls['rol'].setValue(usuario.rol!);

          },
          error: () =>
          {
            console.log('error');
          }
        }
      )
    }

    actualizar()
    {
      if(this.formulario.invalid)
        {
          alert('Los datos contienen errores. Intente nuevamente');
          return;
        }

      const usuario2 = this.formulario.getRawValue();

      usuario2.email = this.usuario?.email!;
      usuario2.password = this.usuario?.password!;

      this.service.putUsuario(usuario2, this.id).subscribe(
        {
          next: ()=>
          {
            alert('Actualizado correctamente');
            this.router.navigate([`perfil-propio-admin/${this.id}`]);
          },
          error: (e: Error)=>{
            alert('Se ha producido un error al actualizar: '+ e.message);
          }
        }
      )
    }

}