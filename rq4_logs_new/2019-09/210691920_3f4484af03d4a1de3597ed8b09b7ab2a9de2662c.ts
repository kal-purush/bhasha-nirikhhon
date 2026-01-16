import {Coordinate, Player, ShotStatus} from "../..";
import {Turn} from "./Turn";

export class GameStatsTracker {

    private _turnPlayed: number = 0;
    private _history: Turn[] = [];

    public logTurn(player: Player, target: Player, coordinate: Coordinate, result: ShotStatus) {
        this._turnPlayed++;
    }

    get turnPlayed(): number {
        return this._turnPlayed;
    }
}