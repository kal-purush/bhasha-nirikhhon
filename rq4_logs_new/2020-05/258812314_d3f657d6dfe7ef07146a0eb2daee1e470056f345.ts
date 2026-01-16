import { GameData } from "types/Game";
import { ScoreEntry } from "types/ScoreEntry";
import Field from "types/Field";
import Player from "types/Player";
import { saveScoreEntries } from "./FrisbeegolfData";
import createInitialScoreEntries from "helpers/createInitialScoreEntries";



export default class Game {
  private _game: GameData;
  private _scoreEntries: ScoreEntry[];

  public constructor(game: GameData, scoreEntries: ScoreEntry[]) {
    this._game = game;
    this._scoreEntries = this.ensureScoreEntries(scoreEntries, game)
  }

  /**
   * Ensure that all scoreentries exists or are created
   * @param scoreEntries The scoreentries to check
   * @param game The game to add scoreentries to
   */
  private ensureScoreEntries(scoreEntries: ScoreEntry[], game: GameData) {
    const expectedNumberOfEntries = game.players.length * game.field.holes.length;
    if(scoreEntries.length === expectedNumberOfEntries) {
      return scoreEntries
    } else {
      let allScoreEntries = [...scoreEntries]
      game.field.holes.forEach(hole => {
        if(!scoreEntries.find(scoreEntry => scoreEntry.hole === hole.number)) {
          let newScoreEntries = createInitialScoreEntries(game.field, hole.number, game.players, game.id || '');
          allScoreEntries.push(...newScoreEntries)
        }
      });
      return allScoreEntries;
    }
  }

  public getField(): Field {
    return this._game.field;
  }

  public getPlayers(): Player[] {
    return this._game.players
  }


  /**
   * Get either all scoreentries, or scoreentries from the given hole.
   * @param hole Optional: the hole from witch to get scoreentries from
   */
  public getScoreEntries(hole?: number): ScoreEntry[] {
    if(hole !== undefined) {
      return this._scoreEntries.filter(scoreEntry => scoreEntry.hole === hole);
    } else {
      return this._scoreEntries;
    }
  }

  /**
   * Update a scoreentry
   * @param scoreEntry The scoreentry to update
   * @param newScore The new score to set on the updated scoreenry
   */
  public updateScoreEntry(playerId: string, hole: number, newScore: number) {
    const newScoreEntries = [...this._scoreEntries];
    let scoreToUpdate = newScoreEntries.findIndex(entry => (entry.playerId === playerId && entry.hole === hole))
    if(scoreToUpdate > -1) {
      newScoreEntries[scoreToUpdate].score = newScore;
      newScoreEntries[scoreToUpdate].updated = true;

      this._scoreEntries = newScoreEntries;
      return newScoreEntries;
    }
  }


  public async saveScoreEntries(holeId: number) {
    await saveScoreEntries(this.getScoreEntries(holeId));
  }
}