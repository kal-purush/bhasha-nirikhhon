import {Component, OnInit} from '@angular/core';
import {User} from '../../models/user.model';
import {Drug} from '../../models/drug.model';
import {ActivatedRoute, Router} from '@angular/router';
import {DarService} from '../../dar/dar.service';
import {FastAppService} from './fastApp.service';
import {AppointmentType} from '../../models/appointmentType.model';
import {DatePipe, Time} from '@angular/common';
import {timepickerReducer} from 'ngx-bootstrap/timepicker/reducer/timepicker.reducer';
import {NgxMaterialTimepicker24HoursFaceComponent, NgxTimepickerFieldComponent} from 'ngx-material-timepicker';
import DateTimeFormat = Intl.DateTimeFormat;
import {NgbTimeStruct} from '@ng-bootstrap/ng-bootstrap';
import {Doctor} from '../../models/doctor.model';
import {Room} from '../../models/room.model';
import {Appointment} from '../../models/appointment.model';

@Component({
  selector: 'app-fastapp',
  templateUrl: './fastApp.component.html',
  styleUrls: ['./fastApp.component.css']
})

export class FastAppComponent implements OnInit {
  appointmenttypes: Set<AppointmentType>;
  doctors: Set<Doctor>;
  user: User;
  doctor: Doctor;
  startDate: Date;
  mindate: Date;
  time: {hour: 13, minute: 30};
  appt: AppointmentType;
  room: Room;
  finalAppointment: Appointment;
  private dt: Date;
  private freerooms: Set<Room>;
  constructor(private router: Router, private route: ActivatedRoute, private service: FastAppService, private datepipe: DatePipe) {
    this.appointmenttypes = new Set<AppointmentType>();
    this.user = new User();
    this.mindate = new Date();
    this.doctor = null;
    this.doctors = new Set<Doctor>();
    this.freerooms = new Set<Room>();
    this.room = null;
    this.finalAppointment = new Appointment();
  }

  ngOnInit(): void {
    const user = JSON.parse(localStorage.getItem('user'));
    this.user = user;
    this.service.getAppointmentTypes(this.user.email).subscribe( data => { this.appointmenttypes = data; });
  }

  get activeconfirm() {
    if (this.startDate == null || this.time == null) {
      return true;
    } else {
      return false;
    }

  }

  showDoctors() {
    // tslint:disable-next-line:max-line-length no-unused-expressionD
    this.doctor = null;
    this.room = null;
    this.dt = new Date(this.startDate);
    this.dt.setHours(this.dt.getHours() + this.time.hour);
    this.dt.setMinutes(this.dt.getMinutes() + this.time.minute + 60);
    const preuzmi = (new Date(this.dt.toLocaleDateString()));
    const datum = this.datepipe.transform(preuzmi, 'yyyy-MM-dd HH:mm:ss');
    this.service.getFreeDr(this.appt, datum.toString(), this.user.email).subscribe(data => {
      this.doctors = data;
      this.service.getFreeRooms(datum.toString(), this.user.email).subscribe(data1 => { this.freerooms = data1; }); });
  }

  get selectedboth() {
    if (this.room != null && this.doctor != null) {
      return true;
    } else {return false; }
  }

  makefa() {
     this.dt = new Date(this.startDate);
     this.dt.setHours(this.dt.getHours() + this.time.hour);
     this.dt.setMinutes(this.dt.getMinutes() + this.time.minute + 60);
     console.log(this.dt);
     this.finalAppointment.startTime = this.dt;
     this.finalAppointment.doctor = this.doctor;
     this.finalAppointment.room = this.room;
     this.finalAppointment.type = this.appt;
    // tslint:disable-next-line:max-line-length
     this.service.makefa(this.finalAppointment, this.user.email).subscribe(result => {location.reload(); alert('Successfully created appointment'); });
  }
}