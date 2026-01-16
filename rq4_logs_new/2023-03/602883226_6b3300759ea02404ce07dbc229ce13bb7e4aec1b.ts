function solution(votes: number[], k: number): number {
  interface canidates {
    [key: number]: number;
  }
  const objectCanidates: canidates = {};
  for (let j = 0; j < votes.length; j++) {
    if (!objectCanidates[votes[j]]) {
      objectCanidates[votes[j]] = 1;
    } else {
      objectCanidates[votes[j]]++;
    }
  }
  const currentLeader: number = Math.max(...votes);
  if (objectCanidates[currentLeader] > 1 && k === 0) {
    return 0;
  }
  let winner = 0;
  for (let i = 0; i < votes.length; i++) {
    if ((votes[i] + k) > currentLeader) {
      winner += 1;
    } else if (k === 0 && votes[i] === currentLeader) {
      return 1;
    }
  }
  return winner;
}

solution([5, 1, 3, 4, 1], 0);