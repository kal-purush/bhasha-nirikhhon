import { Game } from './game.po';
import { UI } from './game.ui.po';
import { AppPage } from '../app.po';
import { Animation } from './game.animation.po';
import { browser } from 'protractor';
import { ITEMS_PER_TURN } from '../../../src/app/core/shared';

const DEFAULT_TIMEOUT = 500;

describe('Game', () => {
  let app: AppPage;
  let game: Game;
  let ui: UI;
  let animation: Animation;

  beforeEach(() => {
    app = new AppPage();
    game = new Game();
    ui = new UI();
    animation = new Animation();

    app.navigateTo();
  });

  describe('on a new turn should', () => {
    it('add new balls to the grid', () => {
      expect(game.getBalls().count()).toBe(ITEMS_PER_TURN, 'add balls');
      expect(animation.getBalls().count()).toBe(ITEMS_PER_TURN, 'add animation');
    });

    it('increment turn number', () => {
      expect(ui.getTurnText()).toBe('1');
    });
  });

  describe('after a turn should', () => {
    it('process matches if exist', () => {});
    it('show leaderboard when the grid is full', () => {});
  });

  describe('on cell selection should', () => {
    it('select a ball if the cell is full', () => {});

    it('move the ball if there is a path', async () => {
      const emptyCell = game.getEmptyCells().first();
      const fullCell = game.getFullCells().first();
      const targetCellId = parseInt(await emptyCell.getId(), 10);

      await browser.sleep(DEFAULT_TIMEOUT);
      await fullCell.click();

      await emptyCell.click();

      expect(await animation.getBalls().count()).toBe(1, 'add animation');

      await browser.sleep(0);

      expect(await fullCell.getText()).toBeFalsy('clear the current cell');
      expect(await game.getCellByNumber(targetCellId)).toBeTruthy('move the ball');
    });

    it('play animation if the path is blocked', () => {});
  });
});