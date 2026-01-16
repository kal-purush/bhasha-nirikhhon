import { Component, OnInit } from '@angular/core';
import { LoggedUserService } from '../Services/logged-user.service';
import { FormBuilder, FormGroup } from '@angular/forms';
import { ToastrService } from 'ngx-toastr';
import { Router } from '@angular/router';
import { NgProgress } from 'ngx-progressbar';
import { TimeSheetService } from '../Services/timeSheet.service';
import * as moment from 'moment';

@Component({
  selector: 'app-create-holiday',
  templateUrl: './create-holiday.component.html',
  styleUrls: ['./create-holiday.component.css'],
  providers: [LoggedUserService]
})
export class CreateHolidayComponent implements OnInit {
  public myForm: FormGroup;

  rtsUser: any;
  rtsCompanyId: any;
  rtsUserId: any;
  userRole: any;
  holidayDate: any;

  constructor(
    private loggedUser: LoggedUserService,
    private formBuilder: FormBuilder,
    private toastr: ToastrService,
    private router: Router,
    private ngProgress: NgProgress,
    private timeSheetService: TimeSheetService,
  ) {
    this.rtsUser = JSON.parse(this.loggedUser.loggedUser);
    this.rtsCompanyId = this.rtsUser.companyId;
    this.rtsUserId = this.rtsUser.userId;
    this.userRole = this.rtsUser.role;
    this.holidayDate = new Date(Date.now());
  }

  ngOnInit() {
    this.myForm = this.formBuilder.group({
      holiday: [''],
      comment: [''],
      // leaveType: [''],
      isHoliday: ['']
    });
  }

  createHoliday(form: FormGroup) {

    this.ngProgress.start();

   const holidayDate = moment(this.holidayDate).format('YYYY-MM-DD');

    const submit = {
      dateId: holidayDate,
      userId: this.rtsUserId,
      comment: form.value.comment,
      leaveType: "Holiday",
      holiday: form.value.isHoliday
    };

    this.timeSheetService.createHoliday(submit)
      .subscribe(
        data => {
            this.ngProgress.done();
            if (data.success) {
            this.toastr.success(data.message, '', {
              positionClass: 'toast-top-center',
              timeOut: 3000,
            });
            this.router.navigate(['holiday-list']);

          } else {
            this.toastr.error(data.message, '', {
              positionClass: 'toast-top-center',
              timeOut: 3000,
            });
          }
        });

  }

}