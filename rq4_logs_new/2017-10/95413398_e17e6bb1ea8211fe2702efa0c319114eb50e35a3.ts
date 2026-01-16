import { Injectable } from '@angular/core';

export interface User {
  Lat: number;
  Lng: number;
  Branch: string;
  Number: number;
  Name: string;
  School: string;
}

@Injectable()
export class MapUserService {

  private name: string;
  private school: string;
  private number: number;
  private branch: string;
  private location: google.maps.LatLngLiteral;


  constructor() {
    this.name = "";
    this.school = "";
    this.location = {lat: 0.0, lng: 0.0};
  }

  public newUser(user:User) {
      this.name = user.Name;
	    this.school = user.School;
      this.number = user.Number;
      this.branch = user.Branch;
	    this.location = {lat: Number(user.Lat), lng: Number(user.Lng)};
  }

  public isValid() {
  	return true;
  }

  public getBranch(){
    return this.branch;
  }

  public getNumber(){
    return this.number;
  }

  public getLocation() {
    if (this.location.lat != 0 && this.location.lng != 0){
  	   return this.location;
    }
  }

  public getLat(){
    return this.location.lat;
  }

  public getLng(){
    return this.location.lng;
  }

}