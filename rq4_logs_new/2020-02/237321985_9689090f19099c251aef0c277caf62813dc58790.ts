import { UsuarioService } from './../../../usuario/usuario.service';
import { Injectable } from '@angular/core';
import { FirebaseDatabase, FirebaseAuth, AngularFireModule } from '@angular/fire';
import { AngularFirestoreCollection } from '@angular/fire/firestore';
import { AngularFireAuth } from '@angular/fire/auth';
import { resolve } from 'url';
import { Usuario } from 'src/app/usuario/model/usuarioModel';

@Injectable({
  providedIn: 'root'
})
export class CadastroService {

  constructor(private firebaseService: AngularFireAuth, private usuarioService:UsuarioService) { }
  CreateAuth(userData){
    return this.firebaseService.auth.createUserWithEmailAndPassword(userData.email,userData.senha).catch(function(error) {
      // Handle Errors here.
      var errorCode = error.code;
      var errorMessage = error.message;
      return "codigo de erro" + errorCode + " mensagem de erro" + errorMessage;
      console.log();
      // ...
    }).then((val:any) => {
      let user = new Usuario(val.user);
      user.nome = userData.nome;
      this.usuarioService.CriarUsuario(user);
      this.usuarioService.ArmazenarLocal(user);
      console.log(user);
    }
      );
  }
  DestroyAuth(){
    this.firebaseService.auth.signOut();
  }
}