export const transformGames = gamesArray => {
  return gamesArray.reduce(
    (acc, curr) => {
      if (!curr.isComplete) {
        return {
          ...acc,
          timeRemaining: acc.timeRemaining + curr.numberOfHoursToComplete,
          numberOfUnfinishedGames: acc.numberOfUnfinishedGames += 1
        };
      } else {
        return {
          ...acc,
          numberOfFinishedGames: acc.numberOfFinishedGames += 1
        };
      }
    },
    {
      timeRemaining: 0,
      numberOfFinishedGames: 0,
      numberOfUnfinishedGames: 0
    }
  );
};