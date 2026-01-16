import { Analyzer } from '../interface/Summery';
import { MatchDataTuple } from '../MatchDataTuple';
import { MatchResult } from '../MatchResult';

export class WinsAnalysis implements Analyzer {
  constructor(public team: string) {}

  run(matchs: MatchDataTuple[]): string {
    let wins = 0;

    for (let match of matchs) {
      if (match[1] === this.team && match[5] === MatchResult.HomeWins) {
        wins++;
      } else if (match[2] === this.team && match[5] === MatchResult.AwayWins) {
        wins++;
      }
    }

    return `Perspolis wins ${wins} games.`;
  }
}