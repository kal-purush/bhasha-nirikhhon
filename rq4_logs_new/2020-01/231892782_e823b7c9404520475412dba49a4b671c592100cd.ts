import { Component, OnInit } from '@angular/core';
import { ApiService } from 'src/app/services/api.service';
import { PubSubService } from 'angular7-pubsub';

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styles: []
})
export class HomeComponent implements OnInit {

  data = [];
  headElements = ['NO.', 'ID', 'AMOUNT', 'PRICE', 'DATE & TIME', 'BUY'];
  constructor(
    private apiservice: ApiService,
    private pubsub :PubSubService
  ) { }
  ngOnInit() {
    this.getdataTable()
  }

  getdataTable() {
    let data = {
      pair: "anx/btc",
      timestamp: 1573212003579
    }
    this.apiservice.getDataTable(data).subscribe(res => {
      if (res && res['data']['trades']) {
        this.data = res['data']['trades'];
      }
    })
  }
  getTimeAndDate(el){
    return new Date(el)
  }
}