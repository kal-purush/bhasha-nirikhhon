import {Component, OnInit} from '@angular/core';

@Component({
  selector: 'app-neumorphism-analog-clock',
  templateUrl: './neumorphism-analog-clock.component.html',
  styleUrls: ['./neumorphism-analog-clock.component.scss']
})
export class NeumorphismAnalogClockComponent implements OnInit {

  deg = 6;
  hr = document.querySelector('#hr');
  mn = document.querySelector('#mn');
  sc = document.querySelector('#sc');

  day = new Date();
  hh = this.day.getHours() * 30;
  mm = this.day.getMinutes() * this.deg;
  ss = this.day.getSeconds() * this.deg;




  constructor() {
    console.log(this.day);

  }

  ngOnInit(): void {
    // this.hr.style.transform = `rotateZ(${(this.hh) + (this.mm/12)}deg)`;
  }

}