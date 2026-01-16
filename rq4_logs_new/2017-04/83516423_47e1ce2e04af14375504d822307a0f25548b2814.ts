import { Component } from '@angular/core';
import { Router } from '@angular/router';
import { AuthHttp } from 'angular2-jwt';
import { MyService} from '../service/myservice';
import { UserName } from '../username';
import { DataService } from '../service/dataservice';
import { OnInit} from '@angular/core';
import { Http, Response, Headers } from '@angular/http';
import { contentHeaders } from '../common/headers';
const styles = require('./adminhome.css');
const template = require('./adminhome.html');

@Component({
  selector: 'adminhome',
  template: template,
  styles: [styles],
  providers: [MyService]
})
export class AdminHome implements OnInit {
  hero : UserName;
  public postObject: any;
  public infoObject: any;
  public balance;
  public walletAddress;
  public email;
  public un;
  public pass;
  public pur;
  constructor(public router: Router,
  public http: Http,
  public authHttp: AuthHttp,
  private myService: MyService,
  private dataservice: DataService) {
  }
  allocate ( event , username ) {
   
    var authHeader = new Headers();
    authHeader.append('Authorization', 'Basic ' +
      btoa(this.dataservice.username + ':' + this.dataservice.password));
      let body = JSON.stringify( this.un, this.pass );
    event.preventDefault();
     this.http.post('http://10.129.12.61:8080/admin/' + username + '/issuelicense' ,
     body, { headers: authHeader})
      .subscribe(
      response => {
        console.log(response.json());
        //alert('User Created Please Login');
      },
      error => {
        alert(error.text());
        console.log(error.text());
        alert('error');
      }
      );
      this.makegetRequest();
  }
  Purchase( purchase ) {

    var authHeader = new Headers();
    authHeader.append('Authorization', 'Basic ' +
      btoa(this.dataservice.username + ':' + this.dataservice.password));
      let body = JSON.stringify( this.un, this.pass );
    event.preventDefault();
     this.http.post('http://10.129.12.61:8080/admin/purchase/' + purchase ,
     body, { headers: authHeader})
      .subscribe(
      response => {
        //alert('User Created Please Login');
      },
      error => {
        console.log(error.text());
        alert('error');
      }
      );
      this.makegetRequest();
  }
  ngOnInit() {
    //this.dataservice.UserName; 
    this.makegetRequest();
}
clicked(event){
  event.preventDefault();
   this.un = "";
   this.pass = "";
   this.router.navigate(['login']);
}
makegetRequest() {
   
    this.un = this.dataservice.username;
    this.pass = this.dataservice.password;

    var authHeader = new Headers();
    authHeader.append('Authorization', 'Basic ' +
      btoa(this.dataservice.username + ':' + this.dataservice.password));
    event.preventDefault();
    let body = JSON.stringify( this.un, this.pass );

    this.http.get('http://10.129.12.61:8080/admin/info', { headers: authHeader})
      .map((data) => data.json())
        .subscribe((data) => {
          this.infoObject = data;
           this.balance = this.infoObject.entity.balance;
          this.walletAddress = this.infoObject.entity.walletAddress;
           this.email = this.infoObject.entity.email;
        });
}
}
