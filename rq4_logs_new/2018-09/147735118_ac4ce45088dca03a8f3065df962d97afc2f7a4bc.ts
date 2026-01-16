import {Component} from '@angular/core';
import {CalendarGeneratorService} from "../../services/calendar-generator/calendar-generator.service";

@Component({
  selector: 'app-root',
  templateUrl: './calendar.html',
  styleUrls: ['./calendar.css']
})
export class Calendar {
  title = 'calendar';

  public model: any;
  constructor(private calendarGen: CalendarGeneratorService) {
  }

  ngOnInit(){
    this.model = this.getCalendar();
  }

  getCalendar() {
    return this.calendarGen.getCurrentModel();
  }

  prevMonth() {
     this.calendarGen.prev();
    this.model = this.getCalendar();
  }

  nextMonth() {
    this.calendarGen.next();
    this.model = this.getCalendar();
  }

}

