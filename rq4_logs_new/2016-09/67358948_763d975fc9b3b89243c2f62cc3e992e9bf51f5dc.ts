/**
 * Created by josecullen on 11/09/16.
 */
import { OnInit } from "@angular/core";
import { EffectivityCircle } from "../effectivity-circle/effectivity-circle.component";
import { GameControl } from "../../services/game-control";
export declare class EffectivityContent implements OnInit {
    private gameControl;
    isGameOver: boolean;
    gameProgress: EffectivityCircle;
    effectivity: EffectivityCircle;
    constructor(gameControl: GameControl);
    ngOnInit(): void;
    ngAfterViewInit(): void;
}