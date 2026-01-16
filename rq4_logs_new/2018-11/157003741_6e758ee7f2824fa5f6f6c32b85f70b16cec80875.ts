import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {API_KEY} from '../app.module';

@Injectable()
export class MapsService {
  constructor(private http: HttpClient) {}

  // Converts from degrees to radians.
  private toRadians(degrees) {
    return degrees * Math.PI / 180;
  };

  /**
   *
   * @param latitude1
   * @param longitude1
   * @param latitude2
   * @param longitude2
   */
  public getDistance(latitude1: number, longitude1: number, latitude2: number, longitude2: number){
    const R = 6371e3; // metres
    const metersToMiles = 0.000621371;
    const φ1 = this.toRadians(latitude1);
    const φ2 = this.toRadians(latitude2);
    const Δφ = this.toRadians(latitude2-latitude1);
    const Δλ = this.toRadians(longitude2-longitude1);

    const a = Math.sin(Δφ/2) * Math.sin(Δφ/2) +
      Math.cos(φ1) * Math.cos(φ2) *
      Math.sin(Δλ/2) * Math.sin(Δλ/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));

    const D = (R * c) * metersToMiles;
    return D.toFixed(2);
  }

  /**
   *
   * @param dateFound
   */
  public getDaysOld(date: string){
    const today = new Date();
    const dateFound = new Date(date);
    // Discard the time and time-zone information.
    const utc1 = Date.UTC(dateFound.getFullYear(), dateFound.getMonth(), dateFound.getDate());
    const utc2 = Date.UTC(today.getFullYear(), today.getMonth(), today.getDate());

    return Math.floor((utc2 - utc1) / (1000 * 60 * 60 * 24));
  }

  /**
   *
   * @param address
   */
  public setLocation(address: string){
    return this.http.get('https://maps.googleapis.com/maps/api/geocode/json?address=' + address + ',+CA&key=AIzaSyAiSO6OQGfj874yQA08Cc8fC5vls3IZDXg');
  }
}