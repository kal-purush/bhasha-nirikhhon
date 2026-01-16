import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-time-overlay',
  templateUrl: './time-overlay.component.html',
  styleUrls: ['./time-overlay.component.css']
})
export class TimeOverlayComponent implements OnInit {

  constructor() { }

  dateTime = null

  ngOnInit() {
    this.dateTime = Date.now()

    setInterval(() => {
      this.dateTime = Date.now()
    }, 1000)
  }

}