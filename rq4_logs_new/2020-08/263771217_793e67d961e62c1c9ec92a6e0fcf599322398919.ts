import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { IPeriod, IShift, IEvent } from '@interfaces/schedule';
import * as moment from 'moment';
import { TimeSelectionComponent } from '@dashboard/components/shared/dialogs/time-selection/time-selection.component';
import {MatDialog, MatDialogConfig } from '@angular/material/dialog';
import { faSignInAlt, faSignOutAlt } from '@fortawesome/free-solid-svg-icons';


@Component({
  selector: 'app-shift-form',
  templateUrl: './shift-form.component.html',
  styleUrls: ['./shift-form.component.sass']
})
export class ShiftFormComponent implements OnInit {

  @Input() day: string;
  @Input() shift: IShift;
  eventDateFrom: string;
  eventDateTo: string;
  eventDate: IEvent | null;
  faSignInAlt = faSignInAlt;
  faSignOutAlt = faSignOutAlt;

  constructor(private dialog: MatDialog) {}

  ngOnInit(): void{
    // const momentDay = moment(this.day);
    this.shift.events.map((event: IEvent) => {
      if(moment(event.fromDatetime, "YYYY-MM-DD").isSame(this.day)){
        this.eventDateFrom = event.fromDatetime;
        this.eventDate = event;
      }
      if(moment(event.toDatetime, "YYYY-MM-DD").isSame(this.day)){
        this.eventDateTo = event.toDatetime;
        this.eventDate = event;
      }
    });
  }


  openDialog() {
    const dialogConfig = new MatDialogConfig();
    dialogConfig.data = { employee: this.shift.employee, cdate: this.day, eventDate: this.eventDate };
    // this.setShiftEvent.emit();

    this.dialog.open(TimeSelectionComponent, dialogConfig)
    .afterClosed()
    .subscribe((reseult: any)  => {
      if (reseult) {
        console.log(reseult.event, "==============DEBUG SUCCESS");
        // this.isDeleting[objective._id] = true;
        // this.objectiveService.deleteObjective(objective._id).subscribe(res => {
        //   this.isDeleted[objective._id] = true;
        //   this.getData(this.search, this.sort, this.pageIndex, this.pageSize);
        // });
      }
    });
  }
}