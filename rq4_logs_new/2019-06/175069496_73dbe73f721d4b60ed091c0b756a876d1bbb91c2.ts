import { Component } from '@angular/core';
import { GroupInfoService } from 'src/app/services/userServices/group-info.service';
import { BetsManager } from 'src/app/models/models';

@Component({
  selector: 'app-manage-football-bet',
  templateUrl: './manage-football-bet.component.html',
  styles: []
})
export class ManageFootballBetComponent  {

  public betsM:BetsManager[];

  constructor(private groupPageS:GroupInfoService) { 
    this.groupPageS.info.subscribe(page=>{
      try{this.betsM = page.manageBets}
      catch(Error){this.betsM = []}
    });
  }

  
}