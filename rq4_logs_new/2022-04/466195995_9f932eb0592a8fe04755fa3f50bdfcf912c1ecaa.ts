import { Injectable } from '@angular/core';
import { FirebaseInfraService } from './firebase-infra.service';
import { User } from './user';

@Injectable({
  providedIn: 'root'
})
export class UserInfaService {

  users:any[]=[];
  
  constructor(public firebaseInfraService: FirebaseInfraService) {
        
    this.updateUsersArray()
  }

  //updating the users local array by subscribe the users observable array.
  private updateUsersArray() {
    this.firebaseInfraService.importCategories();
    this.firebaseInfraService.getUsersObservable.subscribe(a => {
      this.users = a;
    });
  }

   /**
   * for handling the promise returned, use "promise.then((data) =>{'data' hold the wanted user...})"
   * for catching error use "promise.then().catch(e){...handling error...}"
   * @param email of user.
   * @returns Promise object
   */
  public getUserByEmail(email: string): Promise<User> {
    return new Promise((resolve, reject) => {
      resolve(this.users.find(e => e.email === email));
    })
  }

}