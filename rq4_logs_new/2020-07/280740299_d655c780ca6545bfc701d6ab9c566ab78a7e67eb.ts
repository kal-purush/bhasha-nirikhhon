export interface ICardType {
  displayName: string;
  type: string;
  format: RegExp;
  startPattern: RegExp;
  gaps: number[];
  lengths: number[];
  code: ICode;
}

export interface ICode {
  name: string;
  length: number;
}

export interface IError {
  cardNumber: string | false;
  expiryDate: string | false;
  cvc: string | false;
}