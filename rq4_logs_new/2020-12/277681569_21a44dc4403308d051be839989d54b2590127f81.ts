export type JosaType = '을/를' | '은/는' | '이/가' | '과/와' | '으로/로';

function checkJongSung(wordCode: number): boolean {
  return (wordCode - 0xAC00) % 28 > 0;
}

export default function getJosa(word: string, josaType: JosaType): string {
  const strCode = word.charCodeAt(word.length - 1);
  // 한글이 아닌 경우
  if (strCode < 0xAC00 || strCode > 0xD7A3) {
    return word;
  }
  switch (josaType) {
    case '을/를':
      return checkJongSung(strCode) ? '을' : '를';
    case '은/는':
      return checkJongSung(strCode) ? '은' : '는';
    case '이/가':
      return checkJongSung(strCode) ? '이' : '가';
    case '과/와':
      return checkJongSung(strCode) ? '과' : '와';
    case '으로/로':
      return checkJongSung(strCode) ? '으로' : '로';
    default:
      throw new Error('getJosa 함수는 josaType 파라미터가 필요합니다.');
  }
}

export function getJosaWithWord(word: string, josaType: JosaType): string {
  const strCode = word.charCodeAt(word.length - 1);
  // 한글이 아닌 경우
  if (strCode < 0xAC00 || strCode > 0xD7A3) {
    return word;
  }
  switch (josaType) {
    case '을/를':
      return checkJongSung(strCode) ? `${word}을` : `${word}를`;
    case '은/는':
      return checkJongSung(strCode) ? `${word}은` : `${word}는`;
    case '이/가':
      return checkJongSung(strCode) ? `${word}이` : `${word}가`;
    case '과/와':
      return checkJongSung(strCode) ? `${word}과` : `${word}와`;
    case '으로/로':
      return checkJongSung(strCode) ? `${word}으로` : `${word}로`;
    default:
      throw new Error('withJosa 함수는 josaType 파라미터가 필요합니다.');
  }
}