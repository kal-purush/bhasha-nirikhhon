import {Component} from 'angular2/core';
import {OnInit} from 'angular2/core';

import {Car} from './car/car';
import {CarService} from './car/car.service';
import {CarComponent} from './car/car.component';

@Component({
    selector: 'base-app',
    templateUrl: 'app/app.component.html',
    styleUrls: ['static/style/master.css'],
    directives: [CarComponent],
    providers: [CarService]
})
export class AppComponent implements OnInit {
    public title = 'Tour of Cars';
    public cars: Car[];
    public selectedCar: Car;

    constructor(private _carService: CarService) {}

    ngOnInit() {
        this.initCars();
    }

    initCars() {
        this._carService.getCars().then(cars => this.setCars(cars));
    }

    setCars(cars) {
        this.cars = cars;
    }

    onSelect(car: Car) {
        this.selectedCar = car;
    }
}