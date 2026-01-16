function solution(citations) {
  let h = 0;
  citations.sort((a, b) => b - a);
  while (h < citations.length) {
    if (citations[h] <= h) break;
    h++;
  }
  return h;
}