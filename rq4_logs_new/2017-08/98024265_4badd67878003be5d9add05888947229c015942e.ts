import { FootballDataApiHome } from '../models/footballdataapi-table.home.models';
// import { FootballDataApiAway } from '../models/footballdataapi-table.away.models';




export class FootballDataApiStanding{
    
    crestURI: string;
    
    position: number;
    teamName: string;
    playedGames: number;
    points: number;
    goals: number;
    goalsAgainst: number;
    goalDifference: number;
    wins: number;
    draws: number;
    losses: number;
    home: FootballDataApiHome;
    // away: FootballDataApiAway;






    // rank: number;
    // team: string;
    // teamId: number;
    // playedGames: number ;
    // crestURI: string;
    // points: number;
    // goals: number;
    // goalsAgainst: number;
    // goalDifference: number;
}