/// <reference path="GameMap.ts"/>
/// <reference path="GamePlayer.ts"/>

module Cm2k15 {
	export class Game {

    private map: GameMap;
    private player: GamePlayer;

    public constructor(map: HTMLDivElement, input: HTMLTextAreaElement) {
      console.log('game constructor');


      this.player = new GamePlayer();
      this.map = new GameMap(map, this.player);
      this.map.Display();

      input.onkeydown = (e: KeyboardEvent) => {
        if (e.keyCode == 13) {
          var command = input.value;
          input.value = '';
          this.onCommand(command);
          return false;
        }
      };
		}

    private onCommand(command) {
      console.log('Command: ' + command);
      if (command == 'move') {
        this.player.Move();
        this.map.Display();
      }
    }
	}
}