export interface RentIndexListItem {
  name: string
  years: number[]
}

export type RentIndexList = RentIndexListItem[]

export type QuestionType = "Exhaustive" | "UserInput"

export interface BaseQuestion {
  id: string
  text: string
  description: string
}

export interface ExhaustiveQuestion extends BaseQuestion {
  questionType: "Exhaustive"
  possibleAnswers: string[]
}

export interface UserInputQuestion extends BaseQuestion {
  questionType: "UserInput"
  inputType: InputType
}

export type Question = UserInputQuestion | ExhaustiveQuestion

export type AnswerType = "percent"

export interface InputType {
  answerType: AnswerType
  example: number
  exampleMeaning: string
}

export interface QuestionCatalog {
  name: string
  questions: Question[]
}

export interface RentIndex {
  name: string
  year: number
  questionCatalogs: QuestionCatalog[]
}

export interface Address {
  country: string
  locality: string
  postalCode: string
  route: string
  streetNumber: string
}

export interface Answers {
  values: {
    [key: string]: string
  }
}

export interface QuestionCatalogRequest {
  yearOfConstruction: number
  areas: number[]
  address: Address
  answers: Answers
}

export interface RentResult {
  rentIndex: {
    min: number
    max: number
    avg: number
  }
}