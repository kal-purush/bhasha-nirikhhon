
import { Component, OnInit,NgZone,OnChanges } from '@angular/core';
import {Router,ROUTER_DIRECTIVES,provideRouter } from '@angular/router';
import { Mongo } from 'meteor/mongo';
import { Meteor } from 'meteor/meteor';
import { Tracker } from 'meteor/tracker';
import * as moment from 'moment';
import {Observable} from 'rxjs/Observable';
import { REACTIVE_FORM_DIRECTIVES, FormGroup, FormBuilder ,Validators} from '@angular/forms';
import template from './logincomponent.html';
 

@Component({
  selector: 'login',
  template,
  directives: [REACTIVE_FORM_DIRECTIVES,ROUTER_DIRECTIVES]
})

export class LoginComponent implements OnInit,OnChanges {
     addForm: FormGroup;
     email: any;
     password: any;
     message:any;
     showmessage: boolean = false;
     ngOnChanges() {
//         if( Session.get("showmessage")==true)
//          {   console.log("using ngDochekck ");
//              this.showmessage=true;
//              this.message=Session.get("message");
//              console.log(this.message);
//          }
//           if( Session.get("movetohome")==true)
//          {
//              this.showmessage=true;
//              console.log(this.showmessage);
//             this._router.navigate(['/csvtemplate']);
//          }
//          if(this.showmessage==true){
//              console.log("we are getting error somewhere");
//          }
     }
//          Tracker.autorun(() => {
//                        this.zone.run(() => {
//                        this.showmessage; 
//                        this.message;
//            this.party = Parties.findOne(this.partyId);
//                   });                          
//                });   
    constructor(private zone: NgZone,private formBuilder: FormBuilder,private _router:Router){  }
   
  ngOnInit() {
//      *** checking if user is already login ***
    if (Meteor.userId()) {
        this._router.navigate(['/csvtemplate']);
    }
      this.addForm = this.formBuilder.group({
      email: ['', Validators.required],
      password: ['', Validators.required]
    });
                 
  }
   login() {
        var self = this;
    if (this.addForm.valid) {
        console.log(this.addForm.controls['email'].value); 
        this.email=this.addForm.controls['email'].value;
        this.password=this.addForm.controls['password'].value;
//        console.log(this.addForm.controls.password.value);
       Meteor.loginWithPassword(this.email,this.password, function(error){
            if (Meteor.user()) {
               console.log(Meteor.userId());
               console.log("this meteor username is working");
//               Session.set("movetohome",true);
                 self._router.navigate(['/csvtemplate']);
            } else {
//               console.log("ERROR: " + error.reason);              
//               console.log(this.message);
                  self.showmessage=true;   
                  self.message=error.reason;  
                  console.log(self.message);
                  Session.set("showmessage",true);
                  Session.set("message",error.reason);
                  console.log(Session.get("message"));

            }
         });
//          self._router.navigate(['/csvtemplate']);
    }
  }
  
    
  }
  