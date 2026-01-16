import { useEffect } from 'react';
import { useGenre } from '../store/useGenre';
import { Difficulty, useDifficulty } from '../store/useDifficulty';

const useSetDifficulties = () => {
  const { currentGenre } = useGenre();
  const { setDifficulties } = useDifficulty();

  useEffect(() => {
    if (currentGenre === 1) {
      const difficulties: Difficulty[] = [
        { id: 1, difficulty: 'easy', genreId: currentGenre },
        { id: 2, difficulty: 'normal', genreId: currentGenre },
        { id: 3, difficulty: 'hard', genreId: currentGenre },
        { id: 4, difficulty: 'expert', genreId: currentGenre },
        { id: 5, difficulty: 'master', genreId: currentGenre },
        { id: 6, difficulty: 'append', genreId: currentGenre },
      ];
      setDifficulties(difficulties);
    } else if (currentGenre === 2) {
      const difficulties: Difficulty[] = [
        { id: 1, difficulty: 'normal', genreId: currentGenre },
        { id: 2, difficulty: 'hard', genreId: currentGenre },
        { id: 3, difficulty: 'extra', genreId: currentGenre },
        { id: 4, difficulty: 'stella', genreId: currentGenre },
        { id: 5, difficulty: 'olivier', genreId: currentGenre },
      ];
      setDifficulties(difficulties);
    }
  }, [currentGenre]);
};

export default useSetDifficulties;