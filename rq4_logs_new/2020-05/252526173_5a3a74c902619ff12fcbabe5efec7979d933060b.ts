export class ImagineIfQuestion {
  id: number;
  question: string;
  answer1: string;
  answer2: string;
  answer3: string;
  answer4: string;
  answer5: string;
  answer6: string;

  constructor(args: ImagineIfQuestion = <ImagineIfQuestion>{}) {
    this.id = args.id;
    this.question = args.question;
    this.answer1 = args.answer1;
    this.answer2 = args.answer2;
    this.answer3 = args.answer3;
    this.answer4 = args.answer4;
    this.answer5 = args.answer5;
    this.answer6 = args.answer6;
  }

}