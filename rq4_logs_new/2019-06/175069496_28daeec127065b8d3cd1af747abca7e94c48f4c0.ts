import { Component, Input } from '@angular/core';
import { EndedFootballBet, HistoryUserFootballBet, GroupBet, IconModel, Icons } from 'src/app/models/models';
import { MoneyBack } from 'src/app/models/Bets/MoneyBack';
import { AlertService } from 'src/app/services/visualServices/alert.service';

@Component({
  selector: 'app-user-football-bet-card-info',
  templateUrl: './user-football-bet-card-info.component.html',
  styles: []
})
export class UserFootballBetCardInfoComponent{

  @Input() userBet:EndedFootballBet[] = [];
  @Input() footballBet:GroupBet[] = [];
  @Input() ended:boolean;
  public coin_icon:IconModel = Icons.COIN;
  
  constructor(private alertS:AlertService) { }

  public isJackpotBet(type:string):boolean{
    return type.includes("JACKPOT");
  }

  public isWinnerBet(type:string):boolean{
    return type.includes("WINNER");
  }

  public correctPart(type:string):string{
    if(type.includes("FULLTIME")) return "Full match";
    if(type.includes("FIRSTHALF")) return "First half";
    if(type.includes("SECONDHALF")) return "Second half";
  }

  public getWinnerWord(winner:number){
    if(winner==0) return "Draw (X)";
    else if(winner==1) return "Home (1)";
    else if(winner==2) return "Away (2)";
  }

  public getJackpot(bet:GroupBet){
    return bet.minBet*bet.usersJoined;
  }

  public getWinRate(bet:GroupBet, own:HistoryUserFootballBet){
    return Math.round((bet.typeBet.winRate+bet.typePay.winRate)*own.bet);
  }

  public  howMuchMoneyBack(typeBetLose:number, typePayLose:number, coins:number){
    return MoneyBack.getMoneyBack(typeBetLose, typePayLose, coins);
  }

  public cancelUserFootballBet(footballBet:GroupBet, coins:number, userFootballBet:string){
    this.alertS.cancelUserFootballBet(footballBet, coins, userFootballBet);
  }
}