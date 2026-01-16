import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-passenger-counter',
  templateUrl: './passenger-counter.component.html',
  styleUrls: ['./passenger-counter.component.scss']
})
export class PassengerCounterComponent implements OnInit {
  counter: number = 1;

  constructor() { }

  ngOnInit() {
  }

  addTraveller() {
    console.log(this.counter)
    this.counter ++;
  }

  removeTraveller() {
    if (this.counter > 0 ) {
      this.counter --;
    }
  }

}