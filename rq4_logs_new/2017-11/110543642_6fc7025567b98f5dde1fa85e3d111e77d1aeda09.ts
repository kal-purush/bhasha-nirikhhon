import { Injectable } from '@angular/core';

@Injectable()
export class Controls {
  public Move(event: KeyboardEvent, character, col) {
    if (!event.altKey) {
      if (event.keyCode === 38 && col) {
        character.moveup();
      }
      if (event.keyCode === 39) {
        character.moveright();
      }
      if (event.keyCode === 37) {
        character.moveleft();
      }
    }
  }

  public StopMove(event: KeyboardEvent, character) {
    if (event.keyCode === 39) {
      character.stopright();
    }
    if (event.keyCode === 37) {
      character.stopleft();
    }
  }
}