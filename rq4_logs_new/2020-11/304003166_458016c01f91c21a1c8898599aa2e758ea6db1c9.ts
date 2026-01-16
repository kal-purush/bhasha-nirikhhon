import { Component, OnInit } from '@angular/core';
import { UsuarioService } from '../services/usuario.service';
import { UsuarioDto } from 'src/app/common/usuario.dto';

@Component({
  selector: 'app-registro',
  templateUrl: './registro.component.html',
  styleUrls: ['./registro.component.css']
})
export class RegistroComponent implements OnInit {
 
    username: string;
    password: string;
    nombre: string;
    apellidos: string;
    email : string;
    telefono: number;

  constructor(private servicioUsuario: UsuarioService) { }

  ngOnInit(): void {

    
  }

  metodoRegistrar(){
 
  const usuario: UsuarioDto = {
    username: this.username,
    password: this.password, 
    nombre: this.nombre, 
    apellidos: this.apellidos, 
    email : this.email, 
    telefono: this.telefono,
  }



  this.servicioUsuario.createUsuario(usuario)


}
}