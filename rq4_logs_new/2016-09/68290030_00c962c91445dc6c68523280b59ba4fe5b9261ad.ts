import { Component, OnInit } from '@angular/core';
import { ROUTER_DIRECTIVES } from '@angular/router';
import { Mongo } from 'meteor/mongo';
import { Meteor } from 'meteor/meteor';
import { REACTIVE_FORM_DIRECTIVES, FormGroup, FormBuilder ,Validators} from '@angular/forms';

import { Productcategory }   from '../../../both/collections/csvdata.collection';

import template from './addproduct.html';
 

@Component({
  selector: 'csvaddproduct',
  template,
  directives: [REACTIVE_FORM_DIRECTIVES,ROUTER_DIRECTIVES]
})

export class CsvAddProductComponent implements OnInit {
  productlist: Mongo.Cursor<any>;
  addForm: FormGroup;
  constructor(private formBuilder: FormBuilder) {}
  
  ngOnInit() {
    this.productlist = Productcategory.find();
    this.addForm = this.formBuilder.group({
      category: ['', Validators.required],
    });
  }
  resetForm() {
    this.addForm.controls['category']['updateValue']('');
  }
 addcategory(){
     if(this.addForm.valid){
//         console.log(this.addForm.valid);
//         console.log(this.addForm.value);
         Productcategory.insert(this.addForm.value);
         
         // to empty the input box
         this.resetForm();
     }
 }
 removeCategory(category){
     Productcategory.remove(category._id);
 }
  
}