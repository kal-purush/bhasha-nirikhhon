import {ICardWithTabs} from '@/types/data';
import {IMovies} from '@/types/movies';
import {ITrendingShows} from '@/types/shows';

export const toCardStateMovies = (data: IMovies[]): ICardWithTabs[] => {
  return data.map((d) => ({
    id: d.id,
    image: d.poster_path,
    vote: d.vote_average,
    title: d.title,
    releaseDate: d.release_date,
  }));
};

export const toCardStateTrendingShows = (data: ITrendingShows[]): ICardWithTabs[] => {
  return data.map((d) => ({
    id: d.id,
    image: d.poster_path,
    vote: d.vote_average,
    title: d.name,
    releaseDate: d.first_air_date,
  }));
};