import { Component, OnInit, Input } from '@angular/core';
import { IPeriod } from '@interfaces/schedule';
import * as moment from 'moment';
import { TimeSelectionComponent } from '@dashboard/components/shared/dialogs/time-selection/time-selection.component';
import {MatDialog, MatDialogConfig } from '@angular/material/dialog';


@Component({
  selector: 'app-event-selection',
  templateUrl: './event-selection.component.html',
  styleUrls: ['./event-selection.component.sass']
})
export class EventSelectionComponent implements OnInit {

  @Input() builder: Array<string[]>;
  @Input() xAxis: string;
  constructor(private dialog: MatDialog) {}


  ngOnInit(): void{

  }


  openDialog() {
    const dialogConfig = new MatDialogConfig();
    dialogConfig.data = { item: `Desea eliminar al objetivo ?`, title: "Eliminar objetivo" };
    // this.subscription.add(
    this.dialog.open(TimeSelectionComponent, dialogConfig)
    .afterClosed()
    .subscribe((success: boolean)  => {
      if (success) {
        console.log(success, "==============DEBUG SUCCESS");
        // this.isDeleting[objective._id] = true;
        // this.objectiveService.deleteObjective(objective._id).subscribe(res => {
        //   this.isDeleted[objective._id] = true;
        //   this.getData(this.search, this.sort, this.pageIndex, this.pageSize);
        // });
      }
    });
    // );
  }
}