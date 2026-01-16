import { Component, OnInit } from '@angular/core';
import { LoggedUserService } from '../Services/logged-user.service';
import { ActivatedRoute } from '@angular/router';
import { NgProgress } from 'ngx-progressbar';
import { ToastrService } from 'ngx-toastr';
import { TimeSheetService } from '../Services/timeSheet.service';
import * as moment from 'moment';
import { UserService } from '../Services/user.service';
import * as _ from 'underscore';
import * as XLSX from 'xlsx';

@Component({
  selector: 'app-timesheet-report',
  templateUrl: './timesheet-report.component.html',
  styleUrls: ['./timesheet-report.component.css'],
  providers: [LoggedUserService]
})
export class TimesheetReportComponent implements OnInit {

  private rtsUser: any;
  private rtsUserId: any;
  private userRole: any;
  private startDate: any;
  private fromDate: Date
  private currentDate: Date
  private rtsCompanyId: any;
  private selectedTimeSheet: any;
  userDetails: any[];
  selectedUser: any;
  selectedTimeSheetLength: any;
  selectedReport: any[];
  selectedUserName: any;

  constructor(
    private loggedUser: LoggedUserService,
    private activatedRoute: ActivatedRoute,
    private ngProgress: NgProgress,
    private toastr: ToastrService,
    private timeSheetService: TimeSheetService,
    private userService: UserService
  ) {
    this.rtsUser = JSON.parse(this.loggedUser.loggedUser);
    this.rtsUserId = this.rtsUser.userId;
    this.userRole = this.rtsUser.role;
    this.selectedTimeSheet = [];
    this.startDate = new Date(Date.now())
    this.currentDate = new Date(Date.now())
    this.selectedUser = this.rtsUserId;
    this.selectedUserName = this.rtsUser.firstName + ' ' + this.rtsUser.lastName;
  }

  ngOnInit() {
    this.ngProgress.start();
    this.getActiveUser();
  }


  getActiveUser() {
    const userId = {
      userId: this.rtsUserId
    };

    this.userService.getActiveUsers(userId)
      .subscribe(
        data => {
          this.ngProgress.done();
          if (data.success) {
            this.userDetails = data.users;
          }
        });
  }

  selectUser(event) {
    const selected = _.findWhere(this.userDetails, { userId: event });
    this.selectedUserName = selected.firstName + ' ' + selected.lastName;
  }

  dateFilter() {
    const fromDate = moment(this.startDate).format('YYYY-MM-DD');
    const toDate = moment(this.currentDate).format('YYYY-MM-DD');

    const days = {
      userId: this.selectedUser,
      fromDate: fromDate,
      toDate: toDate
    };

    this.timeSheetService.getTimeSheetReport(days)
      .subscribe(data => {
        this.ngProgress.done();
        if (data.success) {
          this.selectedTimeSheet = data.daySheets;
          this.selectedTimeSheetLength = this.selectedTimeSheet.length;
        } else {
          this.selectedTimeSheet = [];
          this.selectedTimeSheetLength = 0;
          this.toastr.error(data.message, '', {
            positionClass: 'toast-top-center',
            timeOut: 3000,
          });
        }
      });
  }

  getTimeSheetReport() {
    this.selectedReport = [];
    for (const sheet of this.selectedTimeSheet) {
      this.selectedReport.push({
        'Date': sheet.dateId,
        'Working Hours': sheet.workingHoursStr,
        'Productivity Hours': sheet.productivityHoursStr,
      });
    }
    const data = this.selectedReport;
    const worksheet: XLSX.WorkSheet = XLSX.utils.json_to_sheet(data);
    const workbook: XLSX.WorkBook = { Sheets: { 'data': worksheet }, SheetNames: ['data'] };
    XLSX.writeFile(workbook, this.selectedUserName + '.xlsx', { bookType: 'xlsx', type: 'buffer' });
  }

}