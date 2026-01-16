import {Component, EventEmitter, OnInit, Output} from '@angular/core';

@Component({
  selector: 'app-game-control',
  templateUrl: './game-control.component.html',
  styleUrls: ['./game-control.component.css']
})
export class GameControlComponent implements OnInit {
  gameTime = 0;
  gameClock;

  @Output() gameStarted = new EventEmitter<number>();

  constructor() {
  }

  ngOnInit() {
  }

  onStartGame() {
    console.log(this);
    this.gameClock = setInterval(() => {
      console.log('INSIDE LAMBDA CALLBACK');
      this.gameTime++;
      console.log('incremented by 1 makes  total  count: ' + this.gameTime);
      this.gameStarted.emit(this.gameTime);
    }, 1000);
  }

  onStop() {
    clearInterval(this.gameClock);
    this.gameTime = 0;
  }

}