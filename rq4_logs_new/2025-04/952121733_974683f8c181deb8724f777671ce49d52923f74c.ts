import { Component } from '@angular/core';
import { FormControl, FormGroup, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { GameService } from '../game/game.service';
import { MatSnackBar } from '@angular/material/snack-bar';
import { Router } from '@angular/router';
import { WebSocketService } from '../game/web-socket.service';

@Component({
  selector: 'app-join',
  imports:
    [
      FormsModule,
      ReactiveFormsModule,
      MatCardModule,
      MatFormFieldModule,
      MatInputModule,
      MatButtonModule
    ],
  templateUrl: './join.component.html',
  styleUrl: './join.component.scss'
})
export class JoinComponent {

  joinGameForm = new FormGroup({
    // We allow alphanumeric input and limit the length for a game id.
    gameId: new FormControl('', Validators.compose([
      Validators.required,
      Validators.minLength(24),
      // length of the game id will be 24
      Validators.maxLength(24),
      Validators.pattern('^[A-Fa-f0-9]{24}$')
    ])),
    playerName: new FormControl('', Validators.compose([
      Validators.required,
      Validators.minLength(2),
      // length of the player name must be 2-100 characters
      Validators.maxLength(100),
    ])),

  });

  readonly joinGameValidationMessages = {
    gameId: [
      {type: 'required', message: 'Game ID is required'},
      {type: 'minlength', message: 'Game ID must be at least 24 characters long'},
      {type: 'maxlength', message: 'Game ID cannot be more than 24 characters long'},
      {type: 'pattern', message: 'Game ID must be a 24 character hexadecimal string'},
      {type: 'serverError', message: 'Game ID not found -- try a different Game ID'},
      {type: 'badRequest', message: 'Bad request -- make sure the Game ID is a valid hexadecimal string'}
    ],
    playerName: [
      {type: 'required', message: 'Player name is required'},
      {type: 'minlength', message: 'Name must be at least 2 characters long'},
      {type: 'maxlength', message: 'Name cannot be more than 100 characters long'},
    ],
  };

  constructor(
    private webSocketService: WebSocketService,
    private gameService: GameService,
    private snackBar: MatSnackBar,
    private router: Router) {
  }

  formControlHasError(controlName: string): boolean {
    return this.joinGameForm.get(controlName).invalid &&
      (this.joinGameForm.get(controlName).dirty || this.joinGameForm.get(controlName).touched);
  }

  getErrorMessage(name: keyof typeof this.joinGameValidationMessages): string {
    for(const {type, message} of this.joinGameValidationMessages[name]) {
      if (this.joinGameForm.get(name).hasError(type)) {
        return message;
      }
    }
    return 'Unknown error';
  }

  submitForm() {
    this.gameService.addPlayer(this.joinGameForm.value.gameId, this.joinGameForm.value.playerName).subscribe({
      next: () => {
        this.snackBar.open(
          `Joined game with id: ${this.joinGameForm.value.gameId}`,
          null,
          { duration: 5000 }
        );
      },
      error: err => {
        if (err.status === 400) {
          this.snackBar.open(
            `Tried to join an illegal game – Error Code: ${err.status}\nMessage: ${err.message}`,
            'OK',
            { duration: 5000 }
          );
          this.joinGameForm.controls['gameId'].setErrors({ badRequest: err.message });
        } else if (err.status === 500) {
          this.snackBar.open(
            `The server failed to process your request to join a game. Is the server up? – Error Code: ${err.status}\nMessage: ${err.message}`,
            'OK',
            { duration: 5000 }
          );
          this.joinGameForm.controls['gameId'].setErrors({ serverError: err.message });
        } else {
          this.snackBar.open(
            `An unexpected error occurred – Error Code: ${err.status}\nMessage: ${err.message}`,
            'OK',
            { duration: 5000 }
          );
        }
      },
      complete: () => {
        this.onPlayerAdd();
        this.router.navigate([`/games/${this.joinGameForm.value.gameId}`]);
      }
    });
  }

  onPlayerAdd() {
    const message = {
      type: 'ADD_PLAYER',
      gameId: this.joinGameForm.value.gameId,
      playerName: this.joinGameForm.value.playerName,
    };

    this.webSocketService.sendMessage(message);
  }
}