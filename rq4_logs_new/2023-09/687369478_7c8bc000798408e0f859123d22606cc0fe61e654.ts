import rawQuestions from "../data/questions.json";
import { Question } from "../types";
import { CODE_NOT_FOUND, CODE_OK, hasKey, isQuestion } from "../utility";

function parseJsonQuestions(rawJson: {}) {
  if (!hasKey(rawJson, 'questions') || !Array.isArray(rawJson.questions)) {
    return [];
  }
  return rawJson.questions.filter(isQuestion);
}

function populateDatabase(questions: Question[]) {
  const result: {
    [id: string]: Question
  } = {};
  for (let id = 0; id < questions.length; id++) {
    result[String(id)] = questions[id];
  }
  return result;
}

const questions = populateDatabase(parseJsonQuestions(rawQuestions));

export function getQuestion(id: string) {
  const question = questions[id];
  if (!question) {
    return { code: CODE_NOT_FOUND };
  }
  return {
    code: CODE_OK,
    body: question
  }
}

export function getQuestions() {
  return {
    code: CODE_OK,
    body: Object.values(questions)
  }
}

export function addQuestion(question: Question) {
  const id = String(getQuestions().body.length);
  questions[id] = question;
  return {
    code: CODE_OK,
    body: id
  }
}

export function updateQuestion(id: string, questionUpdates: Partial<Question>) {
  const {
    body: question,
    code
  } = getQuestion(id);
  if (code != CODE_OK) {
    return { code };
  }
  if (!question) {
    return { code: CODE_NOT_FOUND };
  }
  questions[id] = {
    ...question,
    ...questionUpdates
  }
  return { code: CODE_OK };
}

export function deleteQuestion(id: string) {
  const {
    body: question,
    code
  } = getQuestion(id);
  if (code != CODE_OK) {
    return { code };
  }
  if (!question) {
    return { code: CODE_NOT_FOUND };
  }
  delete questions[id];
  return { code: CODE_OK };
}