`
무인도 여행
https://school.programmers.co.kr/learn/courses/30/lessons/154540
`
function solution(maps) {
  // BFS
  let visited = Array.from(Array(maps.length), () => Array(maps[0].length).fill(false));  // 전체 맵에 대해 방문여부 체크하는 거 두고
  const matrix = maps.map(v => [...v]); // 맵도 매트릭스로 변경
  const result = [];
  let sumVal = 0;
  for (let row = 0; row < matrix.length; row++) { // 맵에 모든 부분을 돌면서
    for (let col = 0; col < matrix[0].length; col++) {
      if (visited[row][col]) continue; // 만약 방문한 곳이면 스킵하고
      if (matrix[row][col] === 'X') { // 방문을 안했는데 해당 부분이 X 면
        visited[row][col] = true; // 방문 처리만 하고
      } else { // 방문하지 않았는데 숫자라면
        [visited, sumVal] = getSum(matrix, visited, row, col); // bfs를 통해 섬의 합을 구해서
        result.push(sumVal); // 넣어준다
      }
    }
  }
  return result.length > 0 ? result.sort((a, b) => a - b) : [-1]; // 오름차순 정렬
}
function getSum(matrix, visited, row, col) { // 상하좌우 방문 가능한 값을 찾아 합을 리턴하는 bfs 
  let sumVal = 0;
  const stack = [[row, col]];
  while (stack.length > 0) {
    const [r, c] = stack.pop();
    if (!visited[r][c]) { // 중복 처리 방지를 위해 이미 방문한 곳은 다시 가지 않음
      visited[r][c] = true;
      sumVal += parseInt(matrix[r][c]);
    }
    if (r > 0 && !visited[r - 1][c] && matrix[r - 1][c] !== 'X') { // 상
      stack.push([r - 1, c]);
    }
    if (c > 0 && !visited[r][c - 1] && matrix[r][c - 1] !== 'X') { // 좌
      stack.push([r, c - 1]);
    }
    if (r < matrix.length - 1 && !visited[r + 1][c] && matrix[r + 1][c] !== 'X') { // 하
      stack.push([r + 1, c]);
    }
    if (c < matrix[0].length - 1 && !visited[r][c + 1] && matrix[r][c + 1] !== 'X') { // 우
      stack.push([r, c + 1]);
    }
  }

  return [visited, sumVal];
}