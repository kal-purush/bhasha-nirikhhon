import { IGame } from "../api";
import { analyzeHands, IGameAnalysis } from "./api";
import { gameValid } from "./ScoreHistory";
import { VsHandDelta } from "./vsHandDelta";

export interface VsDeltaHistoryResult {
  games: number;
  total: number;
  deltaHistory: number[];
}

export class VsDeltaHistory implements IGameAnalysis<VsDeltaHistoryResult> {
  private p1Id: string;
  private p2Id: string;

  constructor(p1Id: string, p2Id: string) {
    this.p1Id = p1Id;
    this.p2Id = p2Id;
  }

  public initialState(): VsDeltaHistoryResult {
    return {
      games: 0,
      total: 0,
      deltaHistory: [],
    };
  }

  public analyze(current: VsDeltaHistoryResult, game: IGame): VsDeltaHistoryResult {
    if (!gameValid(game)) {
      return current;
    }
    if (
      game.players != null &&
      game.players.find(p => p != null && p.id === this.p1Id) != null &&
      game.players.find(p => p != null && p.id === this.p2Id)
    ) {
      const handDelta = analyzeHands(game.hands!, new VsHandDelta(this.p1Id, this.p2Id));
      const total = current.total + handDelta.delta;
      current.deltaHistory.push(total);
      current.total = total;
      current.games += 1;
    }
    return current;
  }
}