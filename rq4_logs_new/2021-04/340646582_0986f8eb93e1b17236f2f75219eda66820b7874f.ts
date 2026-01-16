import { Component, OnInit } from '@angular/core';
import {Car} from "../model/car.model";
import {NgbActiveModal} from "@ng-bootstrap/ng-bootstrap";
import {ActivatedRoute} from "@angular/router";
import {CarService} from "../services/car.service";
import {Client} from "../model/client.model";

@Component({
  selector: 'app-add-or-delete-car',
  templateUrl: './add-car-model.html',
  styleUrls: ['./add-car-model.css']
})
export class AddCarModel implements OnInit {

  constructor(public modal: NgbActiveModal,
              private route: ActivatedRoute,
              private carService: CarService) { }


  newCar: Car = new Car();
  isLoading = false;
  profile: Client;


  ngOnInit(): void {
    this.newCar.client = this.profile
  }


  onSubmit() {
    this.isLoading = true;
    this.createCar(this.newCar);
    this.isLoading = false;
    console.log("test1")
    this.modal.close("Yes");
    // window.location.reload();
  }

  createCar(car: Car): void {
    this.carService.create(car)
      .subscribe(() => {}, error => {
        console.log("error get profile");
      });
  }

}