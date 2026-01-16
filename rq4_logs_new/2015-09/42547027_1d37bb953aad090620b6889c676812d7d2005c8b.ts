import {Component, View, Inject, bootstrap} from 'angular2/angular2';
import {ROUTER_DIRECTIVES, Router, RouteParams} from 'angular2/router';
import {GameManager} from './game_manager';
import {KeyboardInputManager} from './keyboard_input_manager';
import {HTMLActuator} from './html_actuator';
import {LocalStorageManager} from './local_storage_manager';
import {LevelManager} from './service/level_manager';

@Component({selector: 'board'})
@View({
    templateUrl: '/client/view/board.ng.html',
    directives: [ROUTER_DIRECTIVES],
})
export class Board
{
    private router;
    private gameManager:GameManager;
    private levelManager:LevelManager;
    private level;

    constructor(
        @Inject(Router) router,
        @Inject(RouteParams) routeParams,
        @Inject(LevelManager) levelManager:LevelManager,
        @Inject(GameManager) gameManager:GameManager
    ) {
        this.router = router;
        this.levelManager = levelManager;
        this.gameManager = gameManager;

        let levelId = routeParams.params.levelId;
        if (!levelId) {
            this.goToRandomLevel();
            return;
        }

        this.level = this.levelManager.getById(levelId);

        // Wait till the browser is ready to render the game (avoids glitches)
        window.requestAnimationFrame(() => {
            this.gameManager.setLevel(this.level);
        });
    }

    public goToRandomLevel() {
        let randomLevel = this.levelManager.getRandomLevel();
        this.router.navigateInstruction(this.router.generate(['/play_level', {levelId: randomLevel.id}]));
    }
}