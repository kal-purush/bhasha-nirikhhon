import { Injectable } from '@angular/core';
const IP_ADDRESS = "http://localhost:3000";

@Injectable({
  providedIn: 'root'
})
export class AuthenticationService {
  generatedChallenge : Object[] = [];
  isAuthenticated : boolean = false;

  constructor() { }



  async generateChallenge(address : string) {
    await this.retrieveChallenge(address);
    return this.generatedChallenge;
  }

  
  async retrieveChallenge(address : string) {
    await fetch(IP_ADDRESS + '/auth/metaMask/' + [address], {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'}
      })
      .catch((error) => {console.log(error)})
      .then((response : Response) => response.json())
      .then((res) => {
        if (res.success) {
          this.generatedChallenge = res.msg
        } else {
          alert("Failed to receive challenge message!")
        }
      });

  }


  async authenticateUser(message : string, signature : string) {
    await fetch(IP_ADDRESS + '/auth/metaMask/' + [message] + "/" + [signature], {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'}
      })
      .catch((error) => {console.log(error)})
      .then((response : Response) => response.json())
      .then((res) => {
        console.log(res);
    });
  }



}