import { ChatGPTAPI, getOpenAIAuth, ChatGPTAPIBrowser } from 'chatgpt';
import dotenv from 'dotenv';
dotenv.config();

const email = process.env.OPENAI_EMAIL;
const password = process.env.OPENAI_PASSWORD;

const api = new ChatGPTAPIBrowser({ email, password });
api.initSession();

export const chatgpt = async (content: string) => {
  // send a message and wait for the response
  let res = await api.sendMessage(content);
  console.log(res.response);

  // send a follow-up
  res = await api.sendMessage('Can you expand on that?', {
    conversationId: res.conversationId,
    parentMessageId: res.messageId,
  });
  console.log(res.response);

  // send another follow-up
  // send a follow-up
  res = await api.sendMessage('What were we talking about?', {
    conversationId: res.conversationId,
    parentMessageId: res.messageId,
  });
  console.log(res.response);
};

// export const chatgpt = (answer: string) => {
//   const answers: string[][] = [[], [], []];
//   const arr = [answers[0].length, answers[1].length, answers[2].length];
//   const min = Math.min(...arr);
//   answers[arr.indexOf(min)].push(answer);

//   if (answers[0].length) {
//     chatGPT1(answers[0][0]);
//   }

//   if (answers[1].length) {
//     chatGPT1(answers[1][0]);
//   }

//   if (answers[2].length) {
//     chatGPT1(answers[2][0]);
//   }

//   async function chatGPT1(answer: string) {
//     // use puppeteer to bypass cloudflare (headful because of captchas)
//     const openAIAuth = await getOpenAIAuth({
//       email: process.env.OPENAI_EMAIL_1,
//       password: process.env.OPENAI_PASSWORD_1,
//       isGoogleLogin: true,
//     });
//     const api = new ChatGPTAPI({ ...openAIAuth });

//     await api.initSession();
//     // send a message and wait for the response
//     const result = await api.sendMessage(answer);
//     // result.response is a markdown-formatted string
//     console.log(result.response);
//     answers[0].shift();
//   }

//   async function chatGPT2(answer: string) {
//     // use puppeteer to bypass cloudflare (headful because of captchas)
//     const openAIAuth = await getOpenAIAuth({
//       email: process.env.OPENAI_EMAIL_2,
//       password: process.env.OPENAI_PASSWORD_2,
//       // isGoogleLogin: true,
//     });
//     const api = new ChatGPTAPI({ ...openAIAuth });

//     await api.initSession();
//     // send a message and wait for the response
//     const result = await api.sendMessage(answer);
//     // result.response is a markdown-formatted string
//     console.log(result.response);
//     answers[1].shift();
//   }

//   async function chatGPT3(answer: string) {
//     // use puppeteer to bypass cloudflare (headful because of captchas)
//     const openAIAuth = await getOpenAIAuth({
//       email: process.env.OPENAI_EMAIL_3,
//       password: process.env.OPENAI_PASSWORD_3,
//       // isGoogleLogin: true,
//     });
//     const api = new ChatGPTAPI({ ...openAIAuth });

//     await api.initSession();
//     // send a message and wait for the response
//     const result = await api.sendMessage(answer);
//     // result.response is a markdown-formatted string
//     console.log(result.response);
//     answers[2].shift();
//   }
// };