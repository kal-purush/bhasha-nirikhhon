import { Injectable } from '@angular/core';
import { UserDto } from '../models/user-dto';

@Injectable({
  providedIn: 'root'
})
export class RegistrationService {

  private url:string = "http://localhost:8090/"

  constructor() { }

  async register(username:string, password:string) {
    console.log(username + password + "in service");
    let udto = new UserDto(username,password);
    console.log(udto);
    let response = await fetch(this.url + "trainers", {
      method: "POST",
      body: JSON.stringify(udto),
      credentials:"include"
    })
    if (response.status == 201){
      console.log("account successfully created");
      //redirect to the landing page or login page
    }
    else{
      console.log("Invalid username or password entered");
    }
  }
}