import { Component } from '@angular/core';
import { Http, Headers, RequestOptions } from '@angular/http';
import { NavController, NavParams } from 'ionic-angular';
import { RegisterPage } from '../register/register';

/*
  Generated class for the Login page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-paymentInfo',
  templateUrl: 'paymentInfo.html'
})
export class PaymentInfoPage {
  //Info input on this page
  public streetNumber: string;
  public streetName: string;
  public streetType: string;
  public postalCode: string;
  public cardName: string;
  public cardType: string;
  public cardNumber: string;
  public cardExpiry: string;

 //Info pushed from previous RegisterPage
  public firstName: string;
  public lastName: string;
  public username: string;
  public password: string;
  public email: string;
 

  constructor(private navCtrl: NavController, private navParams: NavParams, private http: Http) {
    //Gets info from register page
    this.firstName=navParams.get('firstNameParam')
    this.lastName=navParams.get('lastNameParam')
    this.email=navParams.get('emailparam');
    this.username=navParams.get('usernameParam');
    this.password=navParams.get('passwordParam');
    //Info from this page
    this.streetNumber='';
    this.streetName='';
    this.streetType='';
    this.postalCode='';
    this.cardName='';
    this.cardType='';
    this.cardNumber='';
    this.cardExpiry='';        
  }
}