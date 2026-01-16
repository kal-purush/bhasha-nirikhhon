import { Component, OnInit, OnDestroy } from '@angular/core';
import { RequestersClientService } from './../../http/requesters-client.service';
import { Requester } from '../../models/requester';
import { MatTableDataSource } from '@angular/material';
@Component({
  selector: 'app-requesters-manager',
  templateUrl: './requesters-manager.component.html',
  styleUrls: ['./requesters-manager.component.css']
})
export class RequestersManagerComponent implements OnInit, OnDestroy {
  public requesters: Requester[];
  private requetserObservable;
  public requesterdataSource = new MatTableDataSource<Requester>();
  public requesterdataColumns = ['id', 'name', 'email', 'phone', 'city'];
  constructor( private requester: RequestersClientService ) { }

  ngOnInit() {
    this.requetserObservable = this.requester.getAllRequester().subscribe(data => {
      console.log(data);
      this.requesters = data;
      this.requesterdataSource.data = data;
    });
  }
  ngOnDestroy() {
    this.requetserObservable.unsubscribe();
  }

}