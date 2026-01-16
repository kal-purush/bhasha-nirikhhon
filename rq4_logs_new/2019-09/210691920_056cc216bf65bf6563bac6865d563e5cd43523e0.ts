import {Player} from "..";
import {GameStatsTracker} from "./GameStatsTracker";

export class GameResult {
    private readonly _winner: Player;
    private readonly _looser: Player;
    private readonly _tracker: GameStatsTracker;

    constructor(winner: Player, looser: Player, tracker: GameStatsTracker) {
        this._winner = winner;
        this._looser = looser;
        this._tracker= tracker;
    }

    get winner(): Player {
        return this._winner;
    }

    get looser(): Player {
        return this._looser;
    }

    get tracker(): GameStatsTracker {
        return this._tracker;
    }
}