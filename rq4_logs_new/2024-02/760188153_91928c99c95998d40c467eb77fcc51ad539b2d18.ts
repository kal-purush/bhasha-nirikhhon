interface Odds {
  live: {
    "1"?: number | null;
    "2"?: number | null;
    X?: number | null;
  };
  pre: {
    "1"?: number;
    "2"?: number;
    X?: number;
  };
}

interface Competition {
  name: string;
  id: number;
}

interface League {
  name: string | null;
  country_id: number | null;
  id: number | null;
}

export interface Fixture {
  competition: Competition;
  home_id: number;
  home_name: string;
  id: number;
  location: string;
  round: string;
  group_id: number;
  date: string;
  away_id: number;
  league: League;
  league_id: number;
  home_translations: { [key: string]: string };
  odds: Odds;
  competition_id: number;
  time: string;
  away_name: string;
  away_translations: { [key: string]: string };
  h2h: string;
}

export interface FixturesResponse {
  success: boolean;
  data: {
    fixtures: Fixture[];
    next_page: string;
    prev_page: boolean | string;
  };
}

export interface TableFixture {
  id: number;
  homeTeam: string;
  awayTeam: string;
  league: string;
  location: string;
  date: Date;
}