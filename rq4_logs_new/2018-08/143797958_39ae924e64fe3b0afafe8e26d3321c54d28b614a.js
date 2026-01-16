const getPoints = (answers, lives) => {

  let points = 0;

  if (answers.length < 10) {
    return -1;
  }

  if (lives === 0) {
    return 0;
  }

  answers.forEach((it) => {

    if (it.isRight) {
      points++;
      if (it.time <= 30) {
        points++;
      }
    }

  });

  return points;
};

const getResult = (totalResults, gamerResult) => {
  const stat = totalResults.slice();
  const gameResult = Object.assign({}, gamerResult);

  if (gameResult.lives === 0) {
    return `У вас закончились все попытки. Ничего, повезёт в следующий раз!`;
  }

  if (gameResult.time === 0) {
    return `Время вышло! Вы не успели отгадать все мелодии`;
  }

  const points = gameResult.points;

  stat.push(points);

  const compareReversed = (a, b) => b - a;

  stat.sort(compareReversed);

  const place = stat.indexOf(points) + 1;
  const success = Math.floor((stat.length - place) / stat.length * 100);

  return `Вы заняли ${place} место из ${stat.length} игроков. Это лучше, чем у ${success}% игроков`;
};

const getLives = (answers) => {

  let lives = 3;

  answers.forEach((it) => {
    if (!it.isRight) {
      lives--;
    }
  });

  if (lives < 0) {
    lives = 0;
  }

  return lives;
};

export {getPoints, getResult, getLives};