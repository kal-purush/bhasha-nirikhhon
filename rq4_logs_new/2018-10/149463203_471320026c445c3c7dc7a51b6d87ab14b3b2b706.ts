export interface KnownFor {
  vote_average: number;
  vote_count: number;
  id: number;
  video: boolean;
  media_type: string;
  title: string;
  popularity: number;
  poster_path: string;
  original_language: string;
  original_title: string;
  genre_ids: number[];
  backdrop_path: string;
  adult: boolean;
  overview: string;
  release_date: string;
  original_name: string;
  name: string;
  first_air_date: string;
  origin_country: string[];
}

export interface Result {
  popularity: number;
  id: number;
  profile_path: string;
  name: string;
  known_for: KnownFor[];
  adult: boolean;
}

export interface PersonlistResponse {
  page: number;
  total_results: number;
  total_pages: number;
  results: Result[];
}