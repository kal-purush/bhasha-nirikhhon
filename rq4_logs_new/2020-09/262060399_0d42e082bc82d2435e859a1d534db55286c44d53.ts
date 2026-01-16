import { Injectable } from '@angular/core';
import { MatSnackBar } from '@angular/material/snack-bar';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Usuario } from './usuario.model';

@Injectable({
  providedIn: 'root'
})

export class UsuarioService {

  baseUrl = 'http://127.0.0.1:3333/usuario';

   constructor(private snackBar: MatSnackBar, private http: HttpClient) { }


  mostrarMensagem(msg: string) : void {
    this.snackBar.open(msg,'x',{
      duration:3000, // 3 segundos 
      horizontalPosition:"right",
      verticalPosition:"top"

    })
  }

  novo(usuario: Usuario): Observable<Usuario>{
    let url = this.baseUrl +'/novo';
    return this.http.post<Usuario>(url, usuario)
  }
}