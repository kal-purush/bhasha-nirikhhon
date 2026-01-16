import { questions } from "../data/interview-questions";

export const selectedQuestions = (() => {
  // 첫 번째 질문은 처음에 넣고
  const selected = [questions[0]];

  // 두 번째 질문은 마지막에 넣기 위해 따로 저장
  const lastQuestion = questions[1];

  // 나머지 질문들 중에서 세 개를 랜덤으로 선택
  const remainingQuestions = questions.slice(2);
  for (let i = 0; i < 3; i++) {
    const randomIndex = Math.floor(Math.random() * remainingQuestions.length);
    selected.push(remainingQuestions.splice(randomIndex, 1)[0]);
  }

  // 마지막에 두 번째 질문을 넣음
  selected.push(lastQuestion);

  return selected;
})();