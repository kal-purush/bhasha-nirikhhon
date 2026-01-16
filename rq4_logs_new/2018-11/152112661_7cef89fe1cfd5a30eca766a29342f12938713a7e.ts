import { Component, OnInit } from '@angular/core';
import {AuthService} from '../../auth.service';
import {CarModel, TokenModel} from '../../app.component';
import {first} from 'rxjs/operators';

@Component({
  selector: 'app-cars',
  templateUrl: './cars.component.html',
  styleUrls: ['./cars.component.css']
})
export class CarsComponent implements OnInit {
   cars: CarModel[] = [];

  constructor(
      private authService: AuthService
  ) { }

  ngOnInit() {
      const token: TokenModel = {
          accessToken: JSON.parse(localStorage.getItem('currentUser'))
      };

      this.authService.getCars(token)
          .pipe(first())
          .subscribe(
              data => {
                  this.cars = data.cars;
              },
              error => {
                  console.log(error);
              });
  }

}