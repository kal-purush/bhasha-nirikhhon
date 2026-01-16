import { useEffect, useState } from 'react';
import { useAuthContext } from '../app/auth/context/AuthContext';
import { MOVIES, LISTS } from '../constants';
import { Movies, MovieType } from '../types';

type UseMovieListReturn = {
  movies: MovieType[];
  list: ListInfo | null;
  loading: boolean;
  error: string | null;
};

type ListInfo = {
  id: string;
  name: string;
  isPrivate: boolean;
  creatorId: string;
};

export const useListMovies = (listId: string): UseMovieListReturn => {
  const { user } = useAuthContext();
  const [movies, setMovies] = useState<MovieType[]>([]);
  const [list, setList] = useState<ListInfo | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!user || !user.accessToken) {
      setLoading(false);
      return;
    }
    if (!listId) {
      setError('list not found');
      setLoading(false);
      return;
    }

    const { accessToken } = user;

    const fetchData = async () => {
      try {
        const [moviesResponse, listInfoResponse] = await Promise.all([
          fetch(`${MOVIES}?listId=${listId}`, {
            method: 'GET',
            headers: {
              Authorization: `Bearer ${accessToken}`,
            },
          }),
          fetch(`${LISTS}/${listId}`, {
            method: 'GET',
            headers: {
              Authorization: `Bearer ${accessToken}`,
            },
          }),
        ]);

        if (moviesResponse.status !== 200 || listInfoResponse.status !== 200) {
          setError('failed to fetch list data');
          setLoading(false);
          return;
        }

        const { movies }: Movies = await moviesResponse.json();
        const listInfo: ListInfo = await listInfoResponse.json();

        setMovies(movies);
        setList({
          id: listId,
          name: listInfo.name,
          isPrivate: listInfo.isPrivate,
          creatorId: listInfo.creatorId,
        });
      } catch (err) {
        console.error(err);
        setError('an error occurred while fetching list data');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [listId]);

  return {
    movies,
    list,
    loading,
    error,
  };
};