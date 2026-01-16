import solution from './jinsoek';

describe('소문자로 이루어진 문자열을 압축하여, 그 길이를 리턴한다.', () => {
  it('문자열이 aabbaccc이면, 7을 리턴한다.', () => {
    const text = 'aabbaccc';
    const result = solution(text);
    expect(result).toBe(7);
  });

  it('문자열이 ababcdcdababcdcd이면, 9를 리턴한다.', () => {
    const text = 'ababcdcdababcdcd';
    const result = solution(text);
    expect(result).toBe(9);
  });

  it('문자열이 abcabcdede이면, 8을 리턴한다.', () => {
    const text = 'abcabcdede';
    const result = solution(text);
    expect(result).toBe(8);
  });

  it('문자열이 abcabcabcabcdededededede이면, 14를 리턴한다.', () => {
    const text = 'abcabcabcabcdededededede';
    const result = solution(text);
    expect(result).toBe(14);
  });

  it('문자열이 xababcdcdababcdcd이면, 17을 리턴한다.', () => {
    const text = 'xababcdcdababcdcd';
    const result = solution(text);
    expect(result).toBe(17);
  });

  it('문자열이 a이면, 1을 리턴한다.', () => {
    const text = 'a';
    const result = solution(text);
    expect(result).toBe(1);
  });
});