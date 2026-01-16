import {Grid} from "./Grid";
import {RandomPicker} from "../utils/RandomPicker";
import {Player, Team} from "./Player";

export class GameConfig {

    public gridHeigth: number = 10;
    public gridWidth: number = 10;

    static getDefault() {
        return new GameConfig();
    }
}

export class Game {
    private _grid: Grid;
    private _hasStarted: Boolean = false;

    private _playerWhite: Player;
    private _playerBlack: Player;
    private _currentTurn: Team = null;

    start(whitePlayer: Player, blackPlayer: Player, config: GameConfig = GameConfig.getDefault()) {
        if (this._hasStarted) {
            return;
        }

        this._playerWhite = whitePlayer;
        this._playerBlack = blackPlayer;

        this._init(config);
        this._hasStarted = true;
    }

    get hasStarted(): Boolean {
        return this._hasStarted;
    }

    private _init(config: GameConfig = null) {
        this._currentTurn = RandomPicker.randomFromEnum(Team);
        this._grid = new Grid(config.gridHeigth, config.gridWidth);
    }

    public getCurrentPlayer() {
        if (this._currentTurn == Team.BLACK) {
            return this._playerBlack;
        }
        return this._playerWhite;
    }
}