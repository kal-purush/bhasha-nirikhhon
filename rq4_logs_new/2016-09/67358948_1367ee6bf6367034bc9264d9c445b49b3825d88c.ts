/**
 * Created by josecullen on 10/09/16.
 */
import {Component, Input, Output, EventEmitter, Directive} from '@angular/core';
import {GameOverType} from "../models";
import {GameControl} from "../services/game-control";

@Component({
    moduleId: module.id,
    selector: 'game-over',
    template: `
<div class="flex-container">
  <header>
    <h1>Game Over</h1>    
  </header>

  <br>

  <section class="content">
      <!--<md-progress-circle mode="determinate" value="effectivity"></md-progress-circle>-->

      <ng-content select="game-over-body"></ng-content>
  </section>

  <br>

  <footer>
  </footer>
  </div>  


  `,
    styleUrls: ['game-over.component.css']

})
export class GameOverComponent{
    @Input() gameOverType:GameOverType
    effectivity:number = 0


    constructor(private gameControl:GameControl){
        this.gameControl.runEffectivity().subscribe(
            value => this.effectivity = value
        )
    }

    runEffectivity(){

    }



}


@Directive({
    selector: 'game-over-body'
})
export class GameOverBody{}
