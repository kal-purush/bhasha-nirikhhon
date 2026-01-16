import IMatch from '../interfaces/IMatch';
import MatchService from './matchService';
import TeamService from './teamService';

export default class LeaderboardService {
  public static totalPointsCalc(match: IMatch[]) {
    let totalPoints = 0;
    let totalVictories = 0;
    let totalDraws = 0;
    let totalLosses = 0;

    match.forEach((element) => {
      if (element.homeTeamGoals > element.awayTeamGoals) {
        totalPoints += 3;
        totalVictories += 1;
      }
      if (element.homeTeamGoals === element.awayTeamGoals) {
        totalPoints += 1;
        totalDraws += 1;
      }
      totalPoints += 0;
      totalLosses += 1;
    });

    return { totalPoints, totalVictories, totalDraws, totalLosses };
  }

  public static efficiencyCalc(points: number, matches: number) {
    return Number(((points / (matches * 3)) * 100).toFixed(2));
  }

  public static async getResults() {
    const teams = await TeamService.getAll();
    const matches = await MatchService.getAll();

    const finishedMatches = matches.filter((match) => match.inProgress === false);
    const results = await Promise.all(teams.map((team) => {
      const teamMatches = finishedMatches
        .filter((match: IMatch) => team.teamName === match.teamHome?.teamName);
      return this.createResults(teamMatches);
    }));
    return results;
  }

  public static async createResults(matches: IMatch[]) {
    return {
      name: matches[0].teamHome?.teamName,
      totalPoints: this.totalPointsCalc(matches).totalPoints,
      totalGames: matches.length,
      totalVictories: this.totalPointsCalc(matches).totalVictories,
      totalDraws: this.totalPointsCalc(matches).totalDraws,
      totalLosses: this.totalPointsCalc(matches).totalLosses,
      // goalsFavor: number,
      // goalsOwn: number,
      // goalsBalance: number,
      // efficiency: number,
    };
  }
}