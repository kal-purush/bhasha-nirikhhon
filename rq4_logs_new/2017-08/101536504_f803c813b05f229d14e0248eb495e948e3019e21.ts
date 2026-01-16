import { Component, OnInit } from '@angular/core';
import { Router }  from '@angular/router';

@Component({
    selector: 'addmodel-page',
    templateUrl: './addmodel.component.html',
    styleUrls: ['./addmodel.component.scss']
})
export class AddModelComponent  {
    public modelNo: string;
    public modelRAM:any;
    public modelROM:any;
    public companyName: string;
    public companyList = [{name:'Nokia'},{name:'jio'},{name:'samsung'}]

    constructor(
    	
    	public router:Router){
    	
    }
    //on form submit
    onSubmit() {
      console.log("companyName",this.companyName)
    }


    valuechange(event:any){
     
    }
}