import { Component, Input, Output, OnInit } from '@angular/core';

@Component({
  selector: 'app-join-input-date',
  template:  `<input type="date" name="date" required/>
              <select name="time" required>
                <option name="time" value="" hidden>Part of the day</option>
                <option name="time" value="10.00 - 13.00">10.00 - 13.00</option>
                <option name="time" value="13.00 - 16.00">13.00 - 16.00</option>
                <option name="time" value="16.00 - 19.00">16.00 - 19.00</option>
              </select>`,
  styleUrls: ['./join-input-date.component.scss']
})

export class JoinInputDateComponent implements OnInit {

  constructor() { }

  ngOnInit(): void {
  }

}