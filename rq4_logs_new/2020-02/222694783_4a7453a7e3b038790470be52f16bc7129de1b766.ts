import {Component, OnInit} from '@angular/core';
import {ActivatedRoute, Router} from '@angular/router';
import {DarService} from '../../dar/dar.service';
import {User} from '../../models/user.model';
import {Drug} from '../../models/drug.model';
import {AcceptingAppService} from './acceptingApp.service';
import {Appointment} from '../../models/appointment.model';
import {DatePipe} from '@angular/common';
import {Room} from '../../models/room.model';

@Component({
  selector: 'app-appointments',
  templateUrl: './acceptingApp.component.html',
  styleUrls: ['./acceptingApp.component.css']
})

export class AcceptingAppComponent implements OnInit {
  showrooms: boolean;
  appointments: Set<Appointment>;
  freerooms: Set<Room>;
  idclicked: string;
  user: User;
  constructor(private router: Router, private route: ActivatedRoute, private service: AcceptingAppService, private datepipe: DatePipe) {
    this.showrooms = false;
    this.appointments = new Set<Appointment>();
    this.user = new User();
    this.freerooms = new Set<Room>();
  }

  ngOnInit(): void {
    const user = JSON.parse(localStorage.getItem('user'));
    this.user = user;
    this.service.getAppRequests(this.user.email).subscribe(data => { this.appointments = data; });
  }

  converttostr(apt: any) {
    const preuzmi = (new Date(apt));
    return preuzmi.getFullYear() + '-' + preuzmi.getMonth() + '-' + preuzmi.getDate() + '\n' + preuzmi.getHours() +
       ':' + preuzmi.getMinutes();
  }

  getfreerooms(id: string) {
    this.idclicked = id;
    this.service.getFreeRooms(id).subscribe( data => {this.freerooms = data;
                                                      this.showrooms = true; });
  }

  back() {
    this.showrooms = false;
  }

  bookRoom(id: number, idclicked: string) {
    // tslint:disable-next-line:max-line-length
    this.service.bookRoom(id, idclicked).subscribe( data => { alert('Room booked');  this.service.getAppRequests(this.user.email).subscribe(data1 => { this.appointments = data1; }); this.showrooms = false; });
  }

  reject(id: string) {
    // tslint:disable-next-line:max-line-length
    this.service.rejectApp(id).subscribe( data => { this.service.getAppRequests(this.user.email).subscribe(data1 => { this.appointments = data1; }); });
  }
}