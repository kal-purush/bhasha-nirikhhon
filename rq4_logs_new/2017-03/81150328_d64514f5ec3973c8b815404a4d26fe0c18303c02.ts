import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';
import {FormBuilder, FormGroup} from "@angular/forms";
import {ClothingDataService} from "../../providers/clothing-data-service";

/*
  Generated class for the SaveItem page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-save-item',
  templateUrl: 'save-item.html'
})
export class SaveItemPage {

  clothingItemForm: FormGroup;

  public items = [];

  constructor(public navCtrl: NavController,
              public navParams: NavParams,
              public formBuilder: FormBuilder,
              public storage: ClothingDataService) {

    this.clothingItemForm = formBuilder.group({
      file_name: [''],
      piece: [''],
      name: [''],
      warm: [''],
      cold: [''],
      rain: [''],
    })
  }

  save() {
    console.log(this.clothingItemForm.value);
  }

}