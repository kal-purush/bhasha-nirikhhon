import { Component, OnInit } from '@angular/core';
import Phaser from 'phaser';
import { MenuScene } from './scenes/Menu.scene';
import { GameScene } from './scenes/Game.scene';
import { BootUtils } from './scenes/BootUtils.scene';

@Component({
  selector: 'phaser-game',
  standalone: true,
  imports: [],
  template: '<div id="phaser-game-component"></div>',
  styleUrl: './game.component.scss'
})

export class GameComponent implements OnInit {

  public game: Phaser.Game;

  constructor() {

  }

  ngOnInit(): void {
    const config: Phaser.Types.Core.GameConfig = {
      width: 1280,
      height: 720,
      type: Phaser.AUTO,
      parent: "phaser-game-component",
      scene: [
        BootUtils,
        MenuScene,
        GameScene
      ],
      physics: {
        default: 'arcade',
        arcade: {
          gravity: { x: 0, y: 0 },
          debug: true
        }
      }
    };

    this.game = new Phaser.Game(config);
  }

}