import { Component, OnInit,Output,EventEmitter } from '@angular/core';
import { FormControl, FormGroupDirective, NgForm, Validators} from '@angular/forms';
import { ErrorStateMatcher } from '@angular/material';
import { Ad } from '../../../classes/ad';


@Component({
  selector: 'app-newad',
  templateUrl: './newad.component.html',
  styleUrls: ['./newad.component.css']
})
export class NewadComponent implements OnInit {

  constructor() { }
  
  
  private _newAd = new EventEmitter();
  
  
  @Output()
  public get newAd():EventEmitter<any>{
    return this._newAd;
  }



  public makeOrder(name:string,description:string,location:string,price:number,deadline:number):void{
    if(name != "" && price >0 && deadline != (undefined || 0)){
      let deadl:number =  new Date().getTime()+ (deadline *3600000);
      let newAd:Ad = new Ad(name,description,location,price,deadl);
      this._newAd.emit(newAd);
    }


  }

  ngOnInit() {
  }

}