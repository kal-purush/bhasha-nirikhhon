import { Component, OnInit, Inject } from '@angular/core';
import { LoggedUserService } from '../Services/logged-user.service';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';
import { DialogData } from '../time-sheet/time-sheet.component';
import * as _ from 'underscore';


@Component({
  selector: 'app-time-sheet-details',
  templateUrl: './time-sheet-details.component.html',
  styleUrls: ['./time-sheet-details.component.css'],
  providers: [LoggedUserService]
})
export class TimeSheetDetailsComponent implements OnInit {

  rtsUser: any;
  rtsUserId: any;
  weekSheets: any[];
  selectedTimeSheet: any[];
  date: any;

  constructor(
    public dialogRef: MatDialogRef<TimeSheetDetailsComponent>,
    @Inject(MAT_DIALOG_DATA) public data: DialogData,
    private loggedUser: LoggedUserService,
  ) {
    this.rtsUser = JSON.parse(this.loggedUser.loggedUser);
    this.rtsUserId = this.rtsUser.userId;
  }

  ngOnInit() {
    this.weekSheets = this.data.weekSheet;
    this.date = this.data.date
    const selected = _.findWhere(this.weekSheets, { dateId: this.data.date });
    this.selectedTimeSheet = selected.timeSessions;
    console.log(this.selectedTimeSheet)
  }

}