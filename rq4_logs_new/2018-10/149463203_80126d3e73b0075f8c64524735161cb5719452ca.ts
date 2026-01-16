import { Injectable } from "@angular/core";
import { AngularFireAuth } from "@angular/fire/auth";
import { auth, User } from "firebase";
import { TmdbService } from "../tmdb.service";
import { AngularFireDatabase } from "@angular/fire/database";
import { filter } from "rxjs/operators";
import { Observable } from "rxjs";

export interface ConexionData {
  via;
  email?: string;
  password?: string;
}

@Injectable({
  providedIn: "root"
})
export class UserService {
  private _user: User;
  private connected: boolean = false;
  private dbData: Observable<any>;

  constructor(
    private anAuth: AngularFireAuth,
    private tmdb: TmdbService,
    private db: AngularFireDatabase
  ) {
    this.anAuth.user.pipe(filter(u => !!u)).subscribe(u => {
      this._user = u;
      this.connected=true;
      const listsPath = `users/${u.uid}`;
      const lists = this.db.list(listsPath);
      lists.push("coucou");
      this.dbData = lists.valueChanges();
    });
  }

  public isConnected(): boolean {
    return this.connected;
  }

  public signinVia(data: ConexionData) {
    if (data.via == 1) {
      this.googleLogin();
    }
    if (data.via == 2) {
      this.facebookLogin();
    }
    if (data.via == 3) {
      this.emailSignUp(data.email, data.password);
    }
  }

  public loginVia(data: ConexionData) {
    if (data.via == 1) {
      this.googleLogin();
    }
    if (data.via == 2) {
      this.facebookLogin();
    }
    if (data.via == 3) {
      this.emailLogin(data.email, data.password);
    }
  }

  public googleLogin() {
    const provider = new auth.GoogleAuthProvider();
    return this.oAuthLogin(provider);
  }

  public facebookLogin() {
    const provider = new auth.FacebookAuthProvider();
    return this.oAuthLogin(provider);
  }

  private oAuthLogin(provider: any) {
    return this.anAuth.auth
      .signInWithPopup(provider)
      .then(credential => {
        console.log("Welcome to Firestarter!!!", "success");
        this._user = credential.user;
        this.connected = true;
      })
      .catch(error => this.handleError(error));
  }

  //// Email/Password Auth ////

  public emailSignUp(email: string, password: string) {
    return this.anAuth.auth
      .createUserWithEmailAndPassword(email, password)
      .then(credential => {
        console.log("Welcome new user!", "success");
        this._user = credential.user; // if using firestore
        this.connected = true;
      })
      .catch(error => this.handleError(error));
  }

  public emailLogin(email: string, password: string) {
    return this.anAuth.auth
      .signInWithEmailAndPassword(email, password)
      .then(credential => {
        console.log("Welcome back!", "success");
        this._user = credential.user;
        this.connected = true;
      })
      .catch(error => this.handleError(error));
  }

  // Sends email allowing user to reset password
  public resetPassword(email: string) {
    const fbAuth = auth();
    return fbAuth
      .sendPasswordResetEmail(email)
      .then(() => console.log("Password update email sent", "info"))
      .catch(error => this.handleError(error));
  }

  public signOut() {
    this.anAuth.auth.signOut().then(() => {
      console.log("deconnected");
      //this.router.navigate(['/']);
      this._user = undefined;
      this.connected = false;
    });
  }

  // If error, console log and notify user
  private handleError(error: Error) {
    console.error(error);
    console.log(error.message, "error");
  }
}