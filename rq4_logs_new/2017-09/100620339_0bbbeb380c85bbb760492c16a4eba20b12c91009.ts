import { OnInit } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { ReservationService } from '../service/reservation.service';
import { Component } from '@angular/core';
import { Router } from '@angular/router';
import {DataSource} from '@angular/cdk';
import {Observable} from 'rxjs/Observable';
import 'rxjs/add/observable/of';

@Component({
        selector: 'app-admin',
        templateUrl: './admin.component.html',
        styleUrls: ['./admin.component.css']
    }
)
export class AdminComponent implements OnInit {
    public bookings: any[];
    constructor(private http: HttpClient, private reservationService: ReservationService,
    private router: Router) {
    }
    ngOnInit() {
        this.adminOverview();
    }
    public adminOverview() {
        this.reservationService.listAllBooking().map(res => res.json()).subscribe((data: any) => {
            this.bookings = data;
            console.log(data);
        }, (error: any) => {
            if (error.status === 403) {
                alert ('You are not admin');
            }
        });
    }

    public sendEmail() {
        this.reservationService.sendEmail().map(res => res.json()).subscribe( () => {
        });
    }
    public navBooking() {
        this.router.navigate(['/create']);
      }
      public navOverview() {
        this.router.navigate(['/overview']);
      }
      public navAdmin() {
        this.router.navigate(['/admin']);
      }
}

export class TableBasicExample {
    displayedColumns = ['checkinDate', 'checkoutDate', 'bookingID', 'username'];
    dataSource = new ExampleDataSource();
  }
const data: Element[] = [{checkinDate: '2017-09-08', checkoutDate: '2019-08-08', bookingID: 116, username: 'rose'}] ;
export interface Element {
    checkinDate: string;
    checkoutDate: string;
    bookingID: number;
    username: string;
  }
export class ExampleDataSource extends DataSource<any> {
  /** Connect function called by the table to retrieve one stream containing the data to render. */
  connect(): Observable<Element[]> {
    return Observable.of(data);
  }

  disconnect() {}
}
