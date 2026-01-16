import { AfterViewInit, Component, ElementRef, Input, ViewChild } from '@angular/core';

interface EventItem {
  status?: string;
  date?: string;
  data?: string;
}
@Component({
  selector: 'app-accordion',
  templateUrl: './accordion.component.html',
  styleUrls: ['./accordion.component.scss']
})
export class AccordionComponent implements AfterViewInit {
  events: EventItem[];
  @ViewChild ('ref') ref! :ElementRef
  constructor() {
    this.events = [
        { status: 'Ordered', date: '15/10/2020 10:30', data:'gfgf' },
        { status: 'Processing', date: '15/10/2020 14:00' },
        { status: 'aaaaa', date: '15/10/2020 10:30'},

    ];
}
 ngAfterViewInit(){
   console.log(this.ref.nativeElement)
 }
}

