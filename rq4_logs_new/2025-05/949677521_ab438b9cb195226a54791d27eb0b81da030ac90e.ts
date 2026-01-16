export const saveFantasyMovies = (movies: any[]) => {
  localStorage.setItem('fantasyMovies', JSON.stringify(movies));
};

export const getFantasyMovies = (): any[] => {
  const data = localStorage.getItem('fantasyMovies');
  return data ? JSON.parse(data) : [];
};