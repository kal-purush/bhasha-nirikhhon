import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
// import 'rxjs/add/operator/map';
// import 'rxjs/add/operator/toPromise';
import { map } from 'rxjs/operators';
// import { toPromise } from 'rxjs/operators';

// import 'rxjs/add/operator/toPromise';
import { toPromise } from 'rxjs/operators';

import { Store } from '@ngrx/store';

import { AppState } from './redux/app.state';
import { Car } from './car.model';
import { AddCar, LoadCars } from './redux/cars.action';


@Injectable()
export class CarsService {

  static BASE_URL = 'http://localhost:3000/';

  constructor(
    private http: HttpClient,
    private store: Store<AppState>) {

  }

  loadCars(): void {
    this.http.get(CarsService.BASE_URL + 'cars')
      .toPromise()
      .then( (cars: Car[]) => {
        this.store.dispatch(new LoadCars(cars));
      });
  }

  addCar(car: Car) {
    this.http.post(CarsService.BASE_URL + 'cars', car)
      .toPromise()
      .then((car: Car) => {
        this.store.dispatch(new AddCar(car));
      });
  }

  deleteCar(car: Car) {
    this.http.delete(CarsService.BASE_URL)
  }
}