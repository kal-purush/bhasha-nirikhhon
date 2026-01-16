import { Injectable } from '@angular/core';
import { auth } from 'firebase/app';
import { AngularFireAuth} from '@angular/fire/auth';
import { User} from 'firebase';
import { first } from 'rxjs/operators';

@Injectable()
export class LoginArtistasService {
  public user:User;

  constructor(public afAuth: AngularFireAuth) { }

  async login(email:string, password:string){
    try{
      const result = await this.afAuth.signInWithEmailAndPassword(email,password);
      return result;
    }
    catch (error){console.log(error)}
  }

  async register(email:string, password:string){
    try{
    const result = await this.afAuth.createUserWithEmailAndPassword(email,password);
    return result;
    }
    catch(error){console.log(error);}
  }

  async logout(){
    try{
      await this.afAuth.signOut();
    }
    catch(error){console.log(error)}
  }

  getCurrentUser(){
    return this.afAuth.authState.pipe(first()).toPromise();
  }
}