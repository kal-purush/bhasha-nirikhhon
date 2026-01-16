import Token from './Token';

export default class LocalName extends Token {
  minLength: number = 6;
  maxLength: number = 12;
  nextChar: string = '@';
}