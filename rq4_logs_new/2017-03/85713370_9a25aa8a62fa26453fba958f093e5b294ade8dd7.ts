import {Component,OnInit} from '@angular/core';
import {Car} from "./domain/Car";
import {CarService} from "./service/CarService";



@Component({
    templateUrl: 'app/datatabledemo.html'
})
export class DataTableDemo implements OnInit {

    cars: Car[];

    cols: any[];

    constructor(private carService: CarService) { }

    ngOnInit() {
        this.carService.getCarsSmall().then(cars => this.cars = cars);

        this.cols = [
            {field: 'vin', header: 'Vin'},
            {field: 'year', header: 'Year'},
            {field: 'brand', header: 'Brand'},
            {field: 'color', header: 'Color'}
        ];
    }
}