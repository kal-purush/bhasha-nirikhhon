import { Component, OnInit } from '@angular/core';
import { Mongo } from 'meteor/mongo';
import { Meteor } from 'meteor/meteor';
import * as moment from 'moment';
import template from './adduser.html';
import { REACTIVE_FORM_DIRECTIVES, FormGroup, FormBuilder ,Validators} from '@angular/forms';
import { Accounts } from 'meteor/accounts-base';

@Component({
  selector: 'adduser',
  template,
  directives: [REACTIVE_FORM_DIRECTIVES]
})

export class adduserComponent implements OnInit {
    addForm: FormGroup;
    userlist: Mongo.Cursor<any>;
//    userlist = Meteor.users;
    
  constructor(private formBuilder: FormBuilder){ }
  ngOnInit() {
      this.userlist=Meteor.users.find({});
      console.log(this.userlist);
      

      this.addForm = this.formBuilder.group({
      username: ['', Validators.required],
      email: ['', Validators.required],
      password: ['', Validators.required],
      role:['', Validators.required]
    });
  }
  
  resetForm() {
    this.addForm.controls['username']['updateValue']('');
    this.addForm.controls['email']['updateValue']('');
    this.addForm.controls['password']['updateValue']('');
    this.addForm.controls['role']['updateValue']('');
  }
  
//  **** Adduser function is used to add new user ****
  adduser() {
    if (this.addForm.valid) {
//        *** creating adduser variable form new user ***
      var adduser={
      username: this.addForm.controls['username'].value,
      email : this.addForm.controls['email'].value,
      password : this.addForm.controls['password'].value,
      profile  : {
          //publicly visible fields like firstname goes here
                    role:this.addForm.controls['role'].value,
                    name:this.addForm.controls['username'].value
                    
                   }
      };    
         Meteor.call('adduser',adduser,(error,response)=>{
            if(error){
                console.log(error.reason);
            }
            else{
                console.log(response);
            }
        });
      this.resetForm();
    }
  }
//  **** remove is used to delete a user from user list ***
  removeUser(user){
      console.log(user);
      if(user.profile.role=="user"){
      Meteor.call('removeuser',user);
      }else{
          console.log("you can not delete a  admin");
      }
  }  
  
  }
  