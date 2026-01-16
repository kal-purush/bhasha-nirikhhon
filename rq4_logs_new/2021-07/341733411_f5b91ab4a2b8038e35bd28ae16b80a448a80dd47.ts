// export interface Question {
//   question: string
//   questionId: string
//   answers: {
//     [key: string]: string
//   }
// }

// outermost object, survey response
export interface SurveyQuestions {
  // woosang sunday update: attr will change to survey after woosang updates
  question: Question[]
  dueDate: string
}

export interface Question {
  // woosang sunday update: question will be deprecated and updated to just exposed desc + hash for the question
  question: QuestionMeta
  options: Option[]
}

// woosang sunday update: remove as well
export interface QuestionMeta {
  description: string
  hash: string
}

export interface Option {
  description: string
  hash: string
}