import { GoogleGenerativeAI } from '@google/generative-ai';
import { Step } from './store';

const genAI = new GoogleGenerativeAI(import.meta.env.VITE_GEMINI_API_KEY);
const model = genAI.getGenerativeModel({ model: 'gemini-1.5-flash' });
const systemPrompt = `You are an expert task planner. Your task is to break down a given task into detailed steps and output them in JSON format. The output should be a list of steps that the user needs to follow to complete the task.

**Steps to follow:**

1. Carefully analyze the given task.
2. Identify all necessary sub-tasks required to complete the main task.
3. Order the sub-tasks in a logical and sequential manner.
4. Format the ordered list of sub-tasks into a JSON object with the key "steps".

**Format:
"{
    "steps": [
        "1. Sub-task 1",
        "2. Sub-task 2",
        "3. Sub-task 3"
    ]
}"

**Examples:**

1. User Input: "방 청소"
   AI Output:
   "{
       "steps": [
           "1. 책상 정리",
           "2. 쓰레기통 비우기",
           "3. 물걸레 청소"
       ]
   }"

2. User Input: "요리하기"
   AI Output:
   "{
       "steps": [
           "1. 재료 준비하기",
           "2. 재료 씻기",
           "3. 재료 손질하기",
           "4. 요리하기",
           "5. 접시에 담기"
       ]
   }"

3. User Input: "보고서 작성"
   AI Output:
   "{
       "steps": [
           "1. 주제 선정",
           "2. 자료 조사",
           "3. 목차 작성",
           "4. 초안 작성",
           "5. 검토 및 수정",
           "6. 최종 작성",
           "7. 제출"
       ]
   }"

Take a deep breath and let's work this out in a step by step way to be sure we have the right answer.
`;

export const fetchAI = async (task: string) => {
  const prompt = `System: ${systemPrompt} \nUser: ${task}`;
  const result = await model.generateContent(prompt);
  const text = result.response.text();
  const clearText = text.replace(/```json|```/g, '').trim();
  const steps = JSON.parse(clearText);
  return steps as Step;
};