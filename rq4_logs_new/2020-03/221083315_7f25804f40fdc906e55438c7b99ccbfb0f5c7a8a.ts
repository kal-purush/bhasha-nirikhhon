import { AdminoSnackbar } from './../../../interfaces';
import { AdminoActionService } from './../../../services/action.service';
import { Component, OnInit, ChangeDetectorRef } from '@angular/core';

@Component({
  selector: 'admino-snackbar-container',
  templateUrl: './admino-snackbar-container.component.html',
  styleUrls: ['./admino-snackbar-container.component.scss']
})
export class AdminoSnackbarContainerComponent implements OnInit {

  snackbars: AdminoSnackbar[] = [];

  constructor(private cd: ChangeDetectorRef, private as: AdminoActionService) {
    this.as.snackbarEvent.subscribe((newSnackbars) => {
      for (const sb of newSnackbars) {
        this.snackbars.unshift(sb);
      }
      this.cd.markForCheck();
    });
  }

  remove(snackbar: AdminoSnackbar) {
    this.snackbars.splice(this.snackbars.indexOf(snackbar), 1);
    this.cd.detectChanges();

  }

  ngOnInit() {
  }

}