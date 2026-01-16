import { isAnswerChecked, isAnswerCorrect, isAnswerIncorrect } from './utils';
import { answerType } from '../types';

describe('isAnswerCorrect', () => {
  let answer: answerType;

  beforeEach(() => {
    answer = {
      id: '1',
      correctness: null,
      isCorrect: true,
      content_html: '',
    }
  });

  it('checks for an id match when correctAnswerId is available', () => {
    expect(isAnswerCorrect(answer, '2')).toBe(false);
    expect(isAnswerCorrect(answer, '1')).toBe(true);

    answer.id = 1;
    expect(isAnswerCorrect(answer, 2)).toBe(false);
    expect(isAnswerCorrect(answer, 1)).toBe(true);
  });

  it('checks isCorrect when answer.id or correctAnswerId is unavailable', () => {
    answer = {...answer, id: '', isCorrect: false};
    expect(isAnswerCorrect(answer, '1')).toBe(false);

    answer.isCorrect = true;
    expect(isAnswerCorrect(answer, '')).toBe(true);
  });

  it('checks correctness value', () => {
    answer.correctness = '0.0';
    expect(isAnswerCorrect(answer, answer.id)).toBe(false);

    answer.correctness = '1.0';
    expect(isAnswerCorrect(answer, answer.id)).toBe(true);
  });
});

describe('isAnswerIncorrect', () => {
  let answer: answerType;

  beforeEach(() => {
    answer = {
      id: '1',
      correctness: null,
      isCorrect: true,
      content_html: '',
    }
  });

  it('checks for incorrectness', () => {
    expect(isAnswerIncorrect(answer, '')).toBe(false);
    expect(isAnswerIncorrect(answer, '2')).toBe(false);
    expect(isAnswerIncorrect(answer, '1')).toBe(true);
  });
});

describe('isAnswerChecked', () => {
  let answer: answerType;

  beforeEach(() => {
    answer = {
      id: '1',
      correctness: null,
      isCorrect: true,
      content_html: '',
    }
  });

  it('checks for being a chosenAnswer', () => {
    expect(isAnswerChecked(answer, [])).toBe(false);
    expect(isAnswerChecked(answer)).toBe(false);
    expect(isAnswerChecked(answer, [answer.id])).toBe(true);
  });
});