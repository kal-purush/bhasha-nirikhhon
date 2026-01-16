import { GameStatusService } from "./game-status.service";
import { GameInstance, Score } from "../models";
import { Observable } from "rxjs/Rx";
export declare class GameControl {
    private gameStatusService;
    private gameInstance;
    private _onStart;
    private _onGameInstanceChange;
    constructor(gameStatusService: GameStatusService);
    start(): void;
    pause(): void;
    resume(): void;
    setGameInstance(gameInstance: GameInstance): void;
    getGameInstance(): GameInstance;
    onStart(): Observable<boolean>;
    onGameInstanceChange(): Observable<GameInstance>;
    onScoreChange(): Observable<Score>;
    gameMock: GameInstance;
}