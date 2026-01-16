import { Injectable } from '@angular/core';
import { AuthData } from './auth-data.model';
import { HttpClient } from '@angular/common/http';


@Injectable({ providedIn: 'root' })
export class AuthService {
    constructor(private http: HttpClient) {}

    createUsuario(email: string, senha: string) {
        const authData: AuthData = {email: email, senha: senha}
        this.http.post('http://localhost:3000/api/usuario/cadastro', authData)
        .subscribe(response => {
            console.log(response);
        });
    }
}