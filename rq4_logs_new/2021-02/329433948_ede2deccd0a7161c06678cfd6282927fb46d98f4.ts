import { AirportType } from '../models/types';
import { Injectable, EventEmitter } from '@angular/core';
import { Observable } from 'rxjs';
import { AirportModel } from 'src/app/shared/models/types';

@Injectable({
  providedIn: 'root'
})
export class AirportSelectionService {
  airportSelected = new EventEmitter<{ airport: AirportModel, airportType: AirportType }>();

  emitAirportSelected(airport: AirportModel, airportType: AirportType): void {
    this.airportSelected.next({airport, airportType});
  }

  observable(): Observable<{airport: AirportModel, airportType: AirportType}> {
    return this.airportSelected.asObservable();
  }
}