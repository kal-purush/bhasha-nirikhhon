export interface IQuestion {
  question: string,
  question_img_url: string,
  answers: {
    answer_img_url: string
  }[],
  correct_answer: number
}