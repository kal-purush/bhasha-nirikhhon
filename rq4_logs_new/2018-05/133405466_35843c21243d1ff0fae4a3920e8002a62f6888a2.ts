
import { Injectable } from '@angular/core';
import { Platform } from 'ionic-angular';
import { Storage } from '@ionic/storage';
import { Credenciales } from '../usuario/usuario';


@Injectable()
export class StorageUsuarioProvider {

  UsuarioAutenticado: Credenciales = {}

  constructor(private platform: Platform,
    private storage: Storage) {
    console.log('Hello StorageUsuarioProvider Provider');
  }


  guardarUsuario(credencial: Credenciales) {
    if (this.platform.is('cordova')) {
      console.log("this.storage: "+JSON.stringify(credencial));
      this.storage.set("credencial", credencial);
    } else {
      localStorage.setItem("credencial", JSON.stringify(credencial));
    }
  }

  obtenerUsuario(): Promise<any> {
    console.log("obtenerUsuario")
    return new Promise((resolve, reject) => {
      if (this.platform.is('cordova')) {

        this.storage.get('credencial').then(val => {
          console.log("this.storage.get: "+JSON.stringify(val));
          if (val) {
            this.UsuarioAutenticado = val;
            console.log(JSON.stringify(val));
            resolve(true);
          } else {
            resolve(false);
          }

        });

      } else {
        localStorage.getItem("credencial");
      }
    })
  }
}