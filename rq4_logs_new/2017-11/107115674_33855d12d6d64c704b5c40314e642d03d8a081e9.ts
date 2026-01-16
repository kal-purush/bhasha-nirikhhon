import { Component, OnInit } from '@angular/core';
import {Http} from "@angular/http";

@Component({
  selector: 'my-account',
  templateUrl: './myaccount.component.html',
  styleUrls: ['./myaccount.component.css']
})
export class MyAccountComponent implements OnInit {
  constructor(private http: Http) {  }
  user: any = {};
  defaultShipAddress:any = {};
  otherAddress :any[]= [];
  title : string = "My Account";
  userId: any;
  isLoading = true;
  ngOnInit() {
      this.userId = localStorage.getItem('user');
      this.userAddress();
  }
  userAddress() {
      this.http.get('user/'+ this.userId)
            .subscribe(
                (u: any) => {
                    this.user = u.json();
                    this.isLoading = false;
                    
                    var userShippAddresses = this.user.shipping_address
                    for(let key of userShippAddresses) {

                        if(key.defaultAddress === true)
                            this.defaultShipAddress = key;
                        else 
                            this.otherAddress.push(key);                        
                    }
                },
                err => { if(err) console.log(err + " Something went wrong!!")},
                () =>{
                 }
            )
  }
  makeDefalut(address) {
    var addressId = address.address_id;
    console.log(addressId);
    console.log(this.user);
    for (let key of this.user.shipping_address) {
        if(key.address_id == addressId) {
            key.defaultAddress = true;
        }
        else {
             key.defaultAddress = false;
        }
    }
    console.log(this.user);

    this.http.put('modifyUser/'+ this.userId, JSON.stringify(this.user))
            .subscribe(res=> {
                user => this.user = user
            },
            err=>{},
            ()=>{console.log("error")});
  }
}