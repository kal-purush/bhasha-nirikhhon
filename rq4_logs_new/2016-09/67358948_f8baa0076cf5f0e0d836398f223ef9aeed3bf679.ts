import { OnInit } from '@angular/core';
import { MdIconRegistry } from '@angular2-material/icon';
import { PopupComponent } from "../popup/popup.component";
import { GameStatusService } from "../../services/game-status.service";
import { TimerService } from "../../services/timer.service";
import { CounterConfig } from "../counter/counter.component";
export declare class ToolbarComponent implements OnInit {
    private gameStatus;
    private timerService;
    private mdIconRegistry;
    config: ToolbarConfig;
    popupScore: PopupComponent;
    popupTime: PopupComponent;
    constructor(gameStatus: GameStatusService, timerService: TimerService, mdIconRegistry: MdIconRegistry);
    ngOnInit(): void;
}
export declare class ToolbarConfig {
    timerCounter: CounterConfig;
    scoreCounter: CounterConfig;
    levelCounter: CounterConfig;
    livesCounter: CounterConfig;
    extraTimeCounter: CounterConfig;
    showExtras: boolean;
    showTooltips: boolean;
    constructor(timerCounter?: CounterConfig, scoreCounter?: CounterConfig, levelCounter?: CounterConfig, livesCounter?: CounterConfig, extraTimeCounter?: CounterConfig, showExtras?: boolean, showTooltips?: boolean);
}