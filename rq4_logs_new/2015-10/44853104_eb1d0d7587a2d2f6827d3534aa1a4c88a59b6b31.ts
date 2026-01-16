import { Game } from './game';
import { UI } from './ui';

let game = new Game();

let ui = new UI(game);

ui.render();