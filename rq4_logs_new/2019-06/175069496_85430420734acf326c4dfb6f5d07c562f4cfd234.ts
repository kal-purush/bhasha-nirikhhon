import { Component } from '@angular/core';
import { IconModel, Icons } from 'src/app/models/models';
import { AlertService } from 'src/app/services/visualServices/alert.service';
import { GroupInfoService } from 'src/app/services/userServices/group-info.service';
import { BetService } from 'src/app/services/restServices/bet.service';
import { MoneyBack } from 'src/app/models/Bets/MoneyBack';

@Component({
  selector: 'app-cancell-user-football-bet',
  templateUrl: './cancell-user-football-bet.component.html',
  styles: []
})
/**
 * Class to fill an alert with a form to cancel a user football bet
 * 
 * @class
 */
export class CancellUserFootballBetComponent{

  //
  // ──────────────────────────────────────────────────────────────────────
  //   :::::: C L A S S   V A R S : :  :   :    :     :        :          :
  // ──────────────────────────────────────────────────────────────────────
  //

  /**
   * A filter know if the bet
   * is a group bet or a solo bet
   * 
   * @access public
   * @var {boolean} jackpotBet
   */
  public jackpotBet:boolean = false;

  /**
   * The coins that the user bet
   * at the begining
   * 
   * @access public
   * @var {number} coins_bet
   */
  public coins_bet:number = 0;

  /**
   * The coins that the user
   * will get back
   * 
   * @access public
   * @var {number} coins_return
   */
  public coins_return:number = 0;

  /**
   * Coin icon
   * 
   * @access public
   * @var {IconModel} coin_icon
   */
  public coin_icon:IconModel = Icons.COIN;

  //
  // ─── VARS TO DO THE REQUEST ─────────────────────────────────────────────────────
  //

  /**
   * The name of the group
   * 
   * @access private
   * @var {string} groupName 
   */
  private groupName:string;

  /**
   * The id of the football bet
   * 
   * @access private
   * @var {string} footballBetId
   */
  private footballBetId:string;

  /**
   * The id of the user football bet
   * 
   * @access private
   * @var {string} userFootballBetId
   */
  private userFootballBetId:string;


  //
  // ──────────────────────────────────────────────────────────────────────────
  //   :::::: C O N S T R U C T O R S : :  :   :    :     :        :          :
  // ──────────────────────────────────────────────────────────────────────────
  //
  
  /**
   * @constructor
   * @param {AlertService} _alertS To get the alert info
   * @param {UserService} _userS To do the user requests
   */
  constructor(private _alertS:AlertService, private groupInfo:GroupInfoService, private _betS:BetService) { 
    this._alertS.target.subscribe(ubId=> this.userFootballBetId = ubId);
    this._alertS.fBet.subscribe(bet=>{
      //Check if the bet is a group bet or a solo bet
      this.jackpotBet = bet.bet.typePay.name.includes("JACKPOT");
      //The coins that the user did bet at the begining
      this.coins_bet = bet.userCoins;
      //The id of the bet
      this.footballBetId = bet.bet.bet;
      //The coins that the user will get back
      this.coins_return = MoneyBack.getMoneyBack(bet.bet.typeBet.cancelRate, bet.bet.typePay.cancelRate, this.coins_bet);
    });
    this.groupInfo.info.subscribe(group=>this.groupName = group.name);
  }


  //
  // ──────────────────────────────────────────────────────────────────────────────────
  //   :::::: P U B L I C   F U N C T I O N S : :  :   :    :     :        :          :
  // ──────────────────────────────────────────────────────────────────────────────────
  //
  
  /**
   * Do the request to cancel a user football bet &
   * close the alert
   * 
   * @access public
   */
  public cancelBet(){
    this._alertS.hideAlert();
    //Bootstrap modal close on form submit. So, I have to
    //show 2 modals, so first hide that and in 0.35 seconds
    //send the petition and show the modal of the response
    setTimeout(this.cancelBetReq.bind(this), 350);
  }


  //
  // ────────────────────────────────────────────────────────────────────────────────────
  //   :::::: P R I V A T E   F U N C T I O N S : :  :   :    :     :        :          :
  // ────────────────────────────────────────────────────────────────────────────────────
  //
  
  /**
   * Do the request to do the bet
   * 
   * @access private
   */
  private cancelBetReq(){
    // console.log(this.footballBetId);
    // console.log(this.userFootballBetId);
    this._betS.cancelUserFootballBet({
      "groupName": this.groupName,
      "footballBet": this.footballBetId,
      "userBet": this.userFootballBetId,
    });
  }
}