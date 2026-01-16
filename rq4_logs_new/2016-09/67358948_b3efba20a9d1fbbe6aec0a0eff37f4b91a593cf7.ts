/**
 * Created by josecullen on 10/09/16.
 */
import { OnInit } from "@angular/core";
import { GameStatusService } from "../../services/game-status.service";
import { GameControl } from "../../services/game-control";
export declare class EffectivityCircle implements OnInit {
    private gameStatus;
    private gameControl;
    wholeGame: boolean;
    isGameOver: boolean;
    effectivity: number;
    constructor(gameStatus: GameStatusService, gameControl: GameControl);
    ngOnInit(): void;
    replay(): void;
    private runEffectivity();
    private calculateEffectivity(wholeGame);
}