import  {Component, Input, Output, EventEmitter} from 'angular2/core'
import  {FriendsService} from './friends'

@Component({
  selector: 'name-component',
  providers: [FriendsService], //dont need this here if it is already done in main.ts


  template: `<div> hello my name is {{name}}</div>
            <button (click)="changeName()"> Change</button>

`
})

export class NameComponent{
  @Input() _name:string;
  @Output() nameChange:EventEmitter<string> = new EventEmitter();
  changeName(){

    this.nameChange.emit(this._name + ' !!!');
  }
  constructor(friendsService:FriendsService){
    console.log(friendsService.list);
  }



}