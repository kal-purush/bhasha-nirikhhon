import { Component, AfterViewInit } from '@angular/core';
import { AngularFire } from 'angularfire2';
import { LocalDataSource } from 'ng2-smart-table';
import { SecondGatewayService } from '../../service';

@Component({
  templateUrl: './accountancy.component.html',
  styleUrls: ['./accountancy.component.css']
})
export class AccountancyComponent implements AfterViewInit{
  keyList = [];
  spend:number = 0;
  get: number = 0;
  accountancy = [{
    id: 0,
    date: 'None',
    money: 'None',
    detail: 'None'
  }];
  source: LocalDataSource;

  settings = {
    columns: {
      id: {title: '순번', filter:false},
      date: {title: 'Date', filter : false},
      money:{title: '금액', filter : false, type:'html',
             valuePrepareFunction: (value)=>{
               value = value.replace(',','');
               if(!parseInt(value)){
                 return "<i>Not a number</i>"
               }else{
                 var display = value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                  if(value > 0){
                    return "<b><font color='blue'>" + display + "</font></b>";
                  }else{
                    return "<b><font color='red'>" + display + "</font></b>";
                  }
               }
             }
           },
      detail:{title: '비고', filter : false, type:'html'}
    },
    attr:{
      id: 'my-table'
    }
  };

  constructor(
    private af: AngularFire,
    private authService: SecondGatewayService){

    this.source = new LocalDataSource(this.accountancy);
    af.database.list('/accountancy',{
      query: {
        orderByChild: 'id'
      }
    }).subscribe(snapshot=>{
      this.spend = 0;
      this.get = 0;

      this.keyList = [];
      if(snapshot.length != 0){
        let tempItem = [];
        snapshot.forEach(item=>{
          tempItem.push(item);
          const this_money = parseInt(item.money.replace(',',''));
          if(!isNaN(this_money)){
            if(this_money >= 0){
              this.get += this_money;
            }else{
              this.spend += this_money;
            }
          }

          this.keyList.push(item['$key']);
        });
        this.source.load(tempItem);
      }
    });
  }

  ngAfterViewInit(){
  }

  save(){
    this.source.getAll().then(lists=>{
      var tempKeyList = [];
      var i = -1;
      this.keyList.forEach(key=>{
        // find index
        i = -1;
        lists.forEach((item,index)=>{
          if(item['key'] == key){
            i = index;
          }
        });

        if(i == -1){
          this.af.database.object('/accountancy/'+key).remove();
        }else{
          // update
          this.af.database.object('/accountancy/'+key).update({
            id: lists[i]['id'],
            date: lists[i]['date'],
            money: lists[i]['money'],
            detail: lists[i]['detail']
          });
          tempKeyList.push(key);
        }

      });

      // new items
      lists.forEach(item=>{
        if(!item['key']){
          this.af.database.list('/accountancy').push({
            id: item['id'],
            date: item['date'],
            money: item['money'],
            detail: item['detail']
          });
        }
      });

    });
  }

  toMoneyFormat(num){
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  }

}