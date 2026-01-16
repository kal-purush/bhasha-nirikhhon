import type { GameWordLength } from '../../../domain/game-word-service';
import { getGridTemplateAreas } from './grid-template-service';
import type { ScoreCellsVariant } from './ScoreCells';

describe('getGridTemplateAreas', () => {
  describe.each`
    gameWordLength
    ${6}
    ${7}
  `('WHEN gameWordLength = $gameWordLength', ({ gameWordLength }: { gameWordLength: GameWordLength }) => {
    describe.each`
      scoreCellVariant
      ${'SCORE'}
      ${'CORRECTNESS'}
    `('AND scoreCellVariant = $scoreCellVariant', ({ scoreCellVariant }: { scoreCellVariant: ScoreCellsVariant }) => {
      const expected6ScoreTemplate = `
  "game-word-letter-0 game-word-letter-1 game-word-letter-2 game-word-letter-3 game-word-letter-4 game-word-letter-5    .                "
  "round-0-letter-0   round-0-letter-1   round-0-letter-2   .                  .                  .                     round-0-score    "
  "round-1-letter-0   round-1-letter-1   round-1-letter-2   round-1-letter-3   .                  .                     round-1-score    "
  ".                  round-2-letter-0   round-2-letter-1   round-2-letter-2   round-2-letter-3   .                     round-2-score    "
  ".                  .                  round-3-letter-0   round-3-letter-1   round-3-letter-2   round-3-letter-3      round-3-score    "
  ".                  .                  .                  round-4-letter-0   round-4-letter-1   round-4-letter-2      round-4-score    "
  ".                  .                  round-5-letter-0   round-5-letter-1   round-5-letter-2   round-5-letter-3      round-5-score    "
  ".                  round-6-letter-0   round-6-letter-1   round-6-letter-2   round-6-letter-3   round-6-letter-4      round-6-score    "
  "round-7-letter-0   round-7-letter-1   round-7-letter-2   round-7-letter-3   round-7-letter-4   .                     round-7-score    "
  "round-8-letter-0   round-8-letter-1   round-8-letter-2   round-8-letter-3   round-8-letter-4   round-8-letter-5      round-8-score    "
  ".                  round-9-letter-0   round-9-letter-1   round-9-letter-2   round-9-letter-3   round-9-letter-4    round-9-score    "
  "round-10-letter-0  round-10-letter-1  round-10-letter-2  round-10-letter-3  round-10-letter-4  round-10-letter-5   round-10-score   "
  "bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-label   bonus-point-value"
  "total-score-label  total-score-label  total-score-label  total-score-label  total-score-label  total-score-label   total-score-value"
`;

      const expected6CorrectnessTemplate = `
  "game-word-letter-0 game-word-letter-1 game-word-letter-2 game-word-letter-3 game-word-letter-4 game-word-letter-5    .                        ."
  "round-0-letter-0   round-0-letter-1   round-0-letter-2   .                  .                  .                     round-0-matching-letter  round-0-non-matching-letter"
  "round-1-letter-0   round-1-letter-1   round-1-letter-2   round-1-letter-3   .                  .                     round-1-matching-letter  round-1-non-matching-letter"
  ".                  round-2-letter-0   round-2-letter-1   round-2-letter-2   round-2-letter-3   .                     round-2-matching-letter  round-2-non-matching-letter"
  ".                  .                  round-3-letter-0   round-3-letter-1   round-3-letter-2   round-3-letter-3      round-3-matching-letter  round-3-non-matching-letter"
  ".                  .                  .                  round-4-letter-0   round-4-letter-1   round-4-letter-2      round-4-matching-letter  round-4-non-matching-letter"
  ".                  .                  round-5-letter-0   round-5-letter-1   round-5-letter-2   round-5-letter-3      round-5-matching-letter  round-5-non-matching-letter"
  ".                  round-6-letter-0   round-6-letter-1   round-6-letter-2   round-6-letter-3   round-6-letter-4      round-6-matching-letter  round-6-non-matching-letter"
  "round-7-letter-0   round-7-letter-1   round-7-letter-2   round-7-letter-3   round-7-letter-4   .                     round-7-matching-letter  round-7-non-matching-letter"
  "round-8-letter-0   round-8-letter-1   round-8-letter-2   round-8-letter-3   round-8-letter-4   round-8-letter-5      round-8-matching-letter  round-8-non-matching-letter"
  ".                  round-9-letter-0   round-9-letter-1   round-9-letter-2   round-9-letter-3   round-9-letter-4    round-9-matching-letter  round-9-non-matching-letter"
  "round-10-letter-0  round-10-letter-1  round-10-letter-2  round-10-letter-3  round-10-letter-4  round-10-letter-5   round-10-matching-letter round-10-non-matching-letter"
  "bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-label   bonus-point-value        bonus-point-value"
  "total-score-label  total-score-label  total-score-label  total-score-label  total-score-label  total-score-label   total-score-value        total-score-value"
`;

      const expected7ScoreTemplate = `
  "game-word-letter-0 game-word-letter-1 game-word-letter-2 game-word-letter-3 game-word-letter-4 game-word-letter-5  game-word-letter-6  .                "
  "round-0-letter-0   round-0-letter-1   round-0-letter-2   .                  .                  .                   .                   round-0-score    "
  "round-1-letter-0   round-1-letter-1   round-1-letter-2   round-1-letter-3   .                  .                   .                   round-1-score    "
  ".                  round-2-letter-0   round-2-letter-1   round-2-letter-2   round-2-letter-3   .                   .                   round-2-score    "
  ".                  .                  round-3-letter-0   round-3-letter-1   round-3-letter-2   round-3-letter-3    .                   round-3-score    "
  ".                  .                  .                  round-4-letter-0   round-4-letter-1   round-4-letter-2    round-4-letter-3    round-4-score    "
  ".                  .                  round-5-letter-0   round-5-letter-1   round-5-letter-2   round-5-letter-3    round-5-letter-4    round-5-score    "
  ".                  round-6-letter-0   round-6-letter-1   round-6-letter-2   round-6-letter-3   round-6-letter-4    .                   round-6-score    "
  "round-7-letter-0   round-7-letter-1   round-7-letter-2   round-7-letter-3   round-7-letter-4   .                   .                   round-7-score    "
  "round-8-letter-0   round-8-letter-1   round-8-letter-2   round-8-letter-3   round-8-letter-4   round-8-letter-5    .                   round-8-score    "
  ".                  round-9-letter-0   round-9-letter-1   round-9-letter-2   round-9-letter-3   round-9-letter-4   round-9-letter-5   round-9-score    "
  "round-10-letter-0  round-10-letter-1  round-10-letter-2  round-10-letter-3  round-10-letter-4  round-10-letter-5  round-10-letter-6  round-10-score   "
  "bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-value"
  "total-score-label  total-score-label  total-score-label  total-score-label  total-score-label  total-score-label  total-score-label  total-score-value"
`;

      const expected7CorrectnessTemplate = `
  "game-word-letter-0 game-word-letter-1 game-word-letter-2 game-word-letter-3 game-word-letter-4 game-word-letter-5  game-word-letter-6  .                        ."
  "round-0-letter-0   round-0-letter-1   round-0-letter-2   .                  .                  .                   .                   round-0-matching-letter  round-0-non-matching-letter"
  "round-1-letter-0   round-1-letter-1   round-1-letter-2   round-1-letter-3   .                  .                   .                   round-1-matching-letter  round-1-non-matching-letter"
  ".                  round-2-letter-0   round-2-letter-1   round-2-letter-2   round-2-letter-3   .                   .                   round-2-matching-letter  round-2-non-matching-letter"
  ".                  .                  round-3-letter-0   round-3-letter-1   round-3-letter-2   round-3-letter-3    .                   round-3-matching-letter  round-3-non-matching-letter"
  ".                  .                  .                  round-4-letter-0   round-4-letter-1   round-4-letter-2    round-4-letter-3    round-4-matching-letter  round-4-non-matching-letter"
  ".                  .                  round-5-letter-0   round-5-letter-1   round-5-letter-2   round-5-letter-3    round-5-letter-4    round-5-matching-letter  round-5-non-matching-letter"
  ".                  round-6-letter-0   round-6-letter-1   round-6-letter-2   round-6-letter-3   round-6-letter-4    .                   round-6-matching-letter  round-6-non-matching-letter"
  "round-7-letter-0   round-7-letter-1   round-7-letter-2   round-7-letter-3   round-7-letter-4   .                   .                   round-7-matching-letter  round-7-non-matching-letter"
  "round-8-letter-0   round-8-letter-1   round-8-letter-2   round-8-letter-3   round-8-letter-4   round-8-letter-5    .                   round-8-matching-letter  round-8-non-matching-letter"
  ".                  round-9-letter-0   round-9-letter-1   round-9-letter-2   round-9-letter-3   round-9-letter-4   round-9-letter-5   round-9-matching-letter  round-9-non-matching-letter"
  "round-10-letter-0  round-10-letter-1  round-10-letter-2  round-10-letter-3  round-10-letter-4  round-10-letter-5  round-10-letter-6  round-10-matching-letter round-10-non-matching-letter"
  "bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-label  bonus-point-value        bonus-point-value"
  "total-score-label  total-score-label  total-score-label  total-score-label  total-score-label  total-score-label  total-score-label  total-score-value        total-score-value"
`;

      const expectedTemplate: Record<GameWordLength, Record<ScoreCellsVariant, string>> = {
        6: {
          SCORE: expected6ScoreTemplate,
          CORRECTNESS: expected6CorrectnessTemplate,
        },
        7: {
          SCORE: expected7ScoreTemplate,
          CORRECTNESS: expected7CorrectnessTemplate,
        },
      };

      it('THEN has expected output', () => {
        const actual = getGridTemplateAreas(gameWordLength, scoreCellVariant);
        expect(actual).toBe(expectedTemplate[gameWordLength][scoreCellVariant]);
      });
    });
  });
});