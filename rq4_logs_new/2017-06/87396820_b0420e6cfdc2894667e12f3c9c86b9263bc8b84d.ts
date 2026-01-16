import { Component } from '@angular/core';
import {NavController, NavParams, ViewController} from 'ionic-angular';
import {Score} from "../../providers/score";
import {User} from "../../providers/user";
import DictionaryHelpFunctions from "../../assets/dictionaryHelpFunctions";



@Component({
  selector: 'page-leader-board',
  templateUrl: 'leader-board.html'
})
export class LeaderboardPage {
scores1 = {};
  scores = [];
  user: any;
  userScore : { [id: string] : any} = {};


  constructor(params: NavParams,
              private viewController: ViewController,
              private _score: Score,
              private _user: User) {


  //  this._score.getAllScores().orderByValue().on('child_added', (data) => {
     this._score.getAllScores().orderByValue().limitToLast(10).on('child_added', (data) => {
      this.scores.push(data);
      //this._user.getUser(data.key).then(user => {
        //  this.userScore = user.val();
        });
    this._score.getAllScores().orderByValue().limitToLast(10).on('child_added', (data) => {
        console.log("data key "+data.key);
        console.log("data value"+ data.val());
      //  console.log("user "+ data.);
        this.scores1 = DictionaryHelpFunctions.addToDictionary(this.scores1, data.key, data);
        this.scores1[data.key].lala = data.val();
        this.scores1[data.key].user =this._score.getscoreUser(data.key);
        console.log("scores1 "+ this.scores1);
      console.log("scores1 data key"+ this.scores1[data.key]);
      console.log("scores1 data key score "+ this.scores1[data.key].lala);
      console.log("scores1 data key user "+ this.scores1[data.key].user);
     /* this._user.getUser(data.key).then((userDetail) => {
        this.scores1[data.key].user = userDetail.val();
      });*/

      });
  }

}