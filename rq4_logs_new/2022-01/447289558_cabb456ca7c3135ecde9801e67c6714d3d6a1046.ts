import { Component, OnInit } from "@angular/core";
import { PassengerForComponent } from "../../models/passenger.interface";

import { PassengerDashboardService } from "../../passenger-dashboard.service";

@Component({
  selector: "passenger-dashboard",
  styleUrls: ["passenger-dashboard.component.scss"],
  template: `
    <div>
      <passenger-count [items]="passengers"> </passenger-count>
      <div *ngFor="let pass of passengers">{{ pass.fullname }}</div>

      <passenger-detail
        *ngFor="let passenger of passengers"
        [detail]="passenger"
        (edit)="handleEdit($event)"
        (remove)="handleRemove($event)"
      ></passenger-detail>
    </div>
  `,
})
export class PassengerDashboardComponent implements OnInit {
  passengers: PassengerForComponent[];

  // Dependency resolved automatically using this
  constructor(private passengerService: PassengerDashboardService) {}

  ngOnInit(): void {
    this.passengers = this.passengerService.getPassengers();
  }

  handleRemove(event: PassengerForComponent) {
    this.passengers = this.passengers.filter(
      (passenger: PassengerForComponent) => {
        return passenger.id != event.id;
      }
    );
  }

  handleEdit(event: PassengerForComponent) {
    this.passengers = this.passengers.map(
      (passenger: PassengerForComponent) => {
        if (passenger.id === event.id) {
          passenger = Object.assign({}, passenger, event);
        }
        return passenger;
      }
    );
    console.log(this.passengers);
  }
}