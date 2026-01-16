import { Component, OnInit } from '@angular/core';
import { trigger, style, transition, animate, keyframes, query, stagger } from '@angular/animations';
import { Http, Response, Headers } from '@angular/http';
import { HttpHeaders } from '@angular/common/http';

@Component({
  selector: 'maintenance',
  templateUrl: './maintenance.html',
  styleUrls: ['./maintenance.scss'],
  animations: [

    trigger('listAnimation', [
      transition('* => *', [

        query(':enter', style({ opacity: 0 }), { optional: true }),

        query(':enter', stagger('300ms', [
          animate('1s ease-in', keyframes([
            style({ opacity: 0, transform: 'translateY(-75%)', offset: 0 }),
            style({ opacity: .5, transform: 'translateY(35px)', offset: 0.3 }),
            style({ opacity: 1, transform: 'translateY(0)', offset: 1.0 }),
          ]))]), { optional: true }),

        query(':leave', stagger('300ms', [
          animate('1s ease-in', keyframes([
            style({ opacity: 1, transform: 'translateY(0)', offset: 0 }),
            style({ opacity: .5, transform: 'translateY(35px)', offset: 0.3 }),
            style({ opacity: 0, transform: 'translateY(-75%)', offset: 1.0 }),
          ]))]), { optional: true })

      ])
    ]),
    trigger('explainerAnim', [
      transition('* => *', [
        query('.row', style({ opacity: 0, transform: 'translateX(-40px)' })),

        query('.row', stagger('500ms', [
          animate('800ms 1.2s ease-out', style({ opacity: 1, transform: 'translateX(0)' })),
        ]))



      ])
    ])

  ]
})
export class MaintenanceComponent {
  // items = [];

  // constructor(public http: Http) {
  //   this.items = ['item1', 'item2', 'item3'];
  // }
   constructor(public http: Http) { }


  // pushItem() {
  //   this.items.push('item');

  //   // this.http.get('http://localhost:4500/item')
  //   // .map(res => res.json())
  //   // .subscribe(
  //   //   data =>{
  //   //     console.log(data)
  //   //   },
  //   //   err => console.log(err),
  //   //   () => console.log("here is the item " )
  //   // );
  // }
  // removeItem() {
  //   this.items.pop();
  // }

  item = [];

  ngOnInit() {

    this.http.get('http://localhost:4500/maintenance')
      .map(res => res.json())
      .subscribe(
      data => {
        this.item = data;
        // console.log(data[0].renter)
      },
      err => console.log(err),
      () => console.log("here is the item ")
      );

  }

}