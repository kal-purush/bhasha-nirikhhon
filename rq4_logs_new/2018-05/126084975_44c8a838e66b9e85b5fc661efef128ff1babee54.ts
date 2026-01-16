import Match from '../../models/match.model'
import LeagueSpecialBetSerie from '../../models/leagueSpecialBetSerie.model'
import UserBet from '../../models/userBet.model'
import UserSpecialBetSerie from '../../models/userSpecialBetSerie.model'
import MatchScorer from '../../models/matchScorer.model'
import LeaguePlayer from '../../models/leaguePlayer.model'

export default interface IEvaluator {
  type: string
  evaluateMatch?(result: Match, tip: UserBet): boolean
  evaluateSerie?(result: LeagueSpecialBetSerie, tip: UserSpecialBetSerie): boolean
  evaluateScorer?(matchScorers: MatchScorer[], tip: UserBet): boolean
}