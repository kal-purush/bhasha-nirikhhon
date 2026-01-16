import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { AuthService } from '../../../service/auth.service';
import { UsuariosService } from '../../../service/usuarios.service';
import { DialogoComponent } from '../../Inicio/cuadro-dialogo/cuadro-dialogo.component';
import { MatDialog } from '@angular/material/dialog';

@Component({
  selector: 'app-eliminar-cuentas-admin',
  standalone: true,
  imports: [RouterLink],
  templateUrl: './eliminar-cuentas-admin.component.html',
  styleUrl: './eliminar-cuentas-admin.component.css'
})

export class EliminarCuentasAdminComponent implements OnInit {
 
  constructor(private route: ActivatedRoute, private dialog: MatDialog,private uS: UsuariosService, private router: Router, private auth : AuthService) {}
   idTr : string | undefined;
   idUs : string | undefined;
   ngOnInit() {
    const id = this.route.snapshot.paramMap.get('id');
    this.idUs = this.auth.getUserId();
    if (id) {
      this.idTr = id;
    } else {
      console.error('El parámetro "id" no está presente en la ruta');
    }
  }
  
  confirmarEliminar() {
    this.uS.deleteUsuario(this.idTr).subscribe({
      next: () => {
        this.router.navigate(['/homeAdmin', this.idUs]);
        this.dialog.open(DialogoComponent, {
          panelClass: "custom-dialog-container",
          data: {
            message: "Se ha eliminado la cuenta con éxito."
          }
        },
    )},
      error: (e : Error) => {
        console.error('Error al eliminar la cuenta:', e.message);
      }
    });
  }

}