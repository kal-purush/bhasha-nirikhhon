
import {Component, OnInit} from '@angular/core';
import {Vehicle} from './vehicle';
import {VehicleService} from './vehicle-service';

@Component({
    selector: 'vehicle',
    templateUrl: '../app/vehicle/vehicle-list.html',
    //providers: [EmployeeService]
})
export class VehicleComponent implements OnInit {

    public vehicles: Vehicle[];

    constructor(private vehicleService:VehicleService){
        console.log("Vehicle Service constructor..");
        
    }

    ngOnInit(){
        this.loadVehicles();
        console.log(this.vehicles);
        
    }

    loadVehicles(){
        this.vehicles = this.vehicleService.getVehicles();
    }

}