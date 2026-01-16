import { Component } from '@angular/core';
import { BetService } from 'src/app/services/restServices/bet.service';
import { GroupInfoService } from 'src/app/services/userServices/group-info.service';
import { AvailableBet, FootballMatch } from 'src/app/models/models';
import { FormGroup, FormControl, Validators } from '@angular/forms';

@Component({
  selector: 'app-football-bet',
  templateUrl: './football-bet.component.html',
  styles: []
})
export class FootballBetComponent  {

  private groupName:string = null;
  public explanationBetType:string;
  public explanationPriceType:string;
  public betForm:FormGroup;
  public bets:AvailableBet[];
  public matches : FootballMatch[];
  public allowedBets : string[];
  public mins:number[];
  public maxs:number[];

  private newBet_competitionMatches_launched = false;
  public selectedBet:boolean = false;
  public selectedMatch:boolean = false;
  public selectedPrice:boolean = false; 

  constructor(private groupPageS:GroupInfoService, private betS:BetService) { 
    this.initializeForm();
    this.groupPageS.info.subscribe(page=>{
      try{
        if(this.groupName != page.name && page.name.length > 1){
          this.groupName = page.name;
          if(!page.type) this.getPageGroup(this.groupName);         
        }
      }
      catch(Error){}
    });
  }

  public selectCompetition(competition:number){
    if(!this.newBet_competitionMatches_launched){
      this.newBet_competitionMatches_launched = true;
      (document.querySelector("#newBet_competitionMatches_button") as HTMLElement).click();
    }
    (document.querySelector("#newBet_competitionMatches_select") as HTMLSelectElement).selectedIndex = 0;
    this.selectedMatch = false;
    this.matches = this.bets[competition].matches;
  }

  public selectMatchDay(matchday:number){
    this.resetForm();
    this.allowedBets = this.matches[matchday].allowedTypeBets;
    if(this.selectedMatch){
      (document.querySelector("#newBet_betType_select") as HTMLSelectElement).selectedIndex = 0;
    }
    else this.selectedMatch = true;
  }

  public getExplainBetType(type:string){
    if(type=="FULLTIME_SCORE"){
      this.explanationBetType = "The players must guess the exact result of the match.";
    }
    else if(type=="PARTTIME_SCORE"){
      this.explanationBetType = "The players must guess the exact result of the first half of the match."
    }
    else if(type=="FULLTIME_WINNER"){
      this.explanationBetType = "The players must guess the winner of the match."
    }
    else if(type=="PARTTIME_WINNER"){
      this.explanationBetType = "The players must guess the winner of the first half of the match."
    }
  }

  public getExplainPriceType(type:string){
    if(type=="exactBet"){
      this.explanationPriceType = "The prize will be for the player who hits the exact result.";
    }
    else if(type=="closerBet"){
      this.explanationPriceType = "The prize will be for the player or players who come closest to the exact result"
    }
  }

  public setMaxBet(){
    let max = this.betForm.controls["maxBet"].value;
    let min = this.betForm.controls["minBet"].value;
    if(max == 0 || (max<min && max!=0)){
      let minOut = min/100;
      this.maxs = Array(100-minOut+1).fill(0).map((x,i)=>(i+minOut)*100);
      this.betForm.controls["maxBet"].setValue(0);
    }
  }

  public setMinBet(){
    let max = this.betForm.controls["maxBet"].value;
    let min = this.betForm.controls["minBet"].value;
    if(min.selectedIndex == 0 || (min>max && min!=0)){
      let minOut = max/100;
      this.mins = Array(minOut).fill(0).map((x,i)=>(i+1)*100);
      this.betForm.controls["minBet"].setValue(0);
    }
  }

  private getPageGroup(name:string){
    this.betS.getPageGroup(name).subscribe(
      (bets:AvailableBet[])=> this.bets = bets
    );
  }
  
  private initializeForm(){
    this.maxs = this.mins = Array(100).fill(0).map((x,i)=>(i+1)*100);
    this.betForm = new FormGroup({
      'minBet': new FormControl(
        0,
        [
          Validators.required,
          Validators.min(100),
          Validators.max(10000)
        ]
      ),
      'maxBet': new FormControl(
        0,
        [
          Validators.required,
          Validators.min(100),
          Validators.max(10001)
        ]
      )
    })
  }

  private resetForm(){
    this.maxs = this.mins = Array(100).fill(0).map((x,i)=>(i+1)*100);
    this.selectedBet = false;
    this.selectedPrice = false;
    this.betForm.reset({
      'minBet':0,
      'maxBet':0
    });
  }
}