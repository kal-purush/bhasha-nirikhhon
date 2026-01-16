import { Injectable } from '@angular/core';
import { ConfigService } from '../config/config.service';
import { Http } from '@angular/http';

@Injectable()
export class WorkfromService {
  constructor(private http: Http, private configService: ConfigService) {}

  get workfromPlaces(): string {
    return this.configService.workfromPlaces;
  }

  // TODO: create interface for options
  getPlaces(latitude: number, longitude: number, options?: any) {
    const params = {
      latitude,
      longitude
    };
    return this.http.get(this.workfromPlaces, { params });
  }
}