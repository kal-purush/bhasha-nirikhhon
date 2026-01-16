import {Component, OnInit} from '@angular/core';
import {GameService} from '../service/gameService/game-service';
import {Route, Router} from '@angular/router';

@Component({
  selector: 'app-game',
  templateUrl: 'game.html',
  styleUrls: ['game.css']
})
export class GameComponent implements OnInit {
  private bool: boolean;

  constructor(private gameService: GameService, private route: Router) {
  };

  ngOnInit(): void {
    debugger;
    try {
      this.gameService.isAccess().subscribe(
        data => {
        },
        error2 => {
          this.route.navigate(['home/login']);
        });
    } catch (e) {
      this.route.navigate(['home/login']);
    }

  }

}