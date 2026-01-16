export interface ITeam {
    teamID: number;
    name: string;
    team_points: number;
}

export interface IUser {
    userID: number;
    teamID: number;
    emailAddress: string;
    points: number;
    name?: string;
}