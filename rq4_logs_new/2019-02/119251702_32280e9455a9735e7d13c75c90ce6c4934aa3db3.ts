import {Component, OnInit, Inject} from '@angular/core';
import {MatDialogRef, MAT_DIALOG_DATA, MatSnackBar} from '@angular/material';
import {HttpService} from 'app/shared/services/http.service';
import {ProgressService} from 'app/shared/services/progress.service';

@Component({
  selector: 'app-order-cancel-confirm',
  templateUrl: './order-cancel-confirm.component.html',
  styleUrls: ['./order-cancel-confirm.component.css']
})
export class OrderCancelConfirmComponent implements OnInit {

  message: any;
  cancelAll: boolean = false;

  constructor(public dialogRef: MatDialogRef<OrderCancelConfirmComponent>,
    @Inject(MAT_DIALOG_DATA) public data: any,
    private httpService: HttpService,
    private progressService: ProgressService,
    private snackBar: MatSnackBar) {}

  ngOnInit() {
    this.message = (this.data && this.data.message) ? this.data.message : null;
  }

  isGuest() {
    try {
      return !!this.message.extra.customer_id;
    } catch (error) {
    }
  }

  hasMoreOrderLines() {
    try {
      return !!this.message.extra.hasMoreOrderLines;
    } catch (error) {
    }
  }

  cancel() {
    this.progressService.enable();
    const body: any = {
      id: this.message._id
    };

    if (this.hasMoreOrderLines())
      body.cancelAll = this.cancelAll;

    this.httpService.post('sm/cancelNotExist', body).subscribe(res => {
      this.progressService.disable();
      this.dialogRef.close();
    }, err => {
      this.progressService.disable();
      this.openSnackBar('خطا در لغو سفارش');
    });
  }

  renew() {
    this.progressService.enable();
    this.httpService.post('sm/renewNotExist', {
      id: this.message._id
    }).subscribe(res => {
      this.progressService.disable();
      this.dialogRef.close();
    }, err => {
      this.progressService.disable();
      this.openSnackBar('خطا در از سر گیری فرایند');
    });
  }


  openSnackBar(message: string) {
    this.snackBar.open(message, null, {
      duration: 2000,
    });
  }

}