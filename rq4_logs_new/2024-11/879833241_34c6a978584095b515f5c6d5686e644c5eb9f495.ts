import { Component, OnInit } from '@angular/core';
import { Router, RouterLink } from '@angular/router';
import { AuthService } from '../../../service/auth.service';
import { UsuariosService } from '../../../service/usuarios.service';
import { DialogoComponent } from '../../Inicio/cuadro-dialogo/cuadro-dialogo.component';
import { MatDialog } from '@angular/material/dialog';

@Component({
  selector: 'app-eliminar-cuenta',
  standalone: true,
  imports: [RouterLink],
  templateUrl: './eliminar-cuenta.component.html',
  styleUrl: './eliminar-cuenta.component.css'
})

export class EliminarCuentaComponent implements OnInit {
  idUs? : string
  constructor( private dialog: MatDialog,private uS: UsuariosService, private router: Router, private auth : AuthService) {}
  
  ngOnInit(){
    this.idUs = this.auth.getUserId();
  }
  
  confirmarEliminar() {
    this.uS.deleteUsuario(this.idUs).subscribe({
      next: () => {

        this.auth.LogOut();
        this.router.navigate(['/login']);
        this.dialog.open(DialogoComponent, {
          panelClass: "custom-dialog-container",
          data: {
            message: "Se ha eliminado tu cuenta con éxito"
          }
        },
    )},
      error: (e : Error) => {
        console.error('Error al eliminar la cuenta:', e.message);
      }
    });
  }

}