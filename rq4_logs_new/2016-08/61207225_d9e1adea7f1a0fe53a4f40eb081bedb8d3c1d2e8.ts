import {Component, Inject} from '@angular/core';
import {ActivatedRoute} from "@angular/router";
import {DialogService} from "../../Services/dialog-service";
import {CarsService} from "../../Services/cars-service";
import {CarModal} from "../../Modals/car-modal";

@Component({
    selector: '[car-details]',
    templateUrl: 'Components/Cars/car.tpl.html',
    providers: [DialogService, CarsService]
})

export class CarDetailsComponent {
    private _carModal = new CarModal();
    constructor(
        @Inject(CarsService) private _carsService,
        @Inject(DialogService) private _dialogService
    ) {
        
    }

    private _saveCar(): void {
        if (this._carModal.id === 0) {
            this._carModal.id = this._carsService.cars.length + 1;
            this._carsService.cars.push(this._carModal);
        }
        else {

        }
        this._carModal = new CarModal();
    }

    ngOnInit() {
        this._carModal = this._carsService.selectedCar;
    }
}