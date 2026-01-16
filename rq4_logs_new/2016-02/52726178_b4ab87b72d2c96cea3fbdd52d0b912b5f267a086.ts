import {Component} from 'angular2/core'

import {OnInit} from 'angular2/core';

import {Car} from './car';
import {CarService} from './car.service';
import {CarComponent} from './car.component';

@Component({
    selector: 'car-list',
    templateUrl: 'app/car/car-list.component.html',
    styleUrls: ['static/style/master.css'],
    directives: [CarComponent],
    providers: [CarService]
})
export class CarListComponent implements OnInit {
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