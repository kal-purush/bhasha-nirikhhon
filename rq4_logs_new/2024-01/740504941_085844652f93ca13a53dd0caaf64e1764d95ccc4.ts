import { Component, OnInit } from '@angular/core';
import { Store } from '@ngrx/store';
import { GameService } from '../../services/game.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-game',
  standalone: true,
  imports: [],
  templateUrl: './game.component.html',
  styleUrl: './game.component.scss'
})
export class GameComponent implements OnInit {

  currentGame: any|null;

  constructor(
    private readonly _store: Store<{ game: any }>,
    private readonly _gameService: GameService,
    private readonly _router: Router,
  ){}

  ngOnInit(): void {
    this._store.select(state => state.game.selectedGame).subscribe(g => {
      this.currentGame = g;
      if(!g) {
        alert('Votre adversaire a quitté la partie');
        this._router.navigate(['/']);
      }
    })
  }
}