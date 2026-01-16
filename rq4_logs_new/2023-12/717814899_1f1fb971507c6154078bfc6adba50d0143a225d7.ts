import { tStringConstructor } from "../services/template-strings";

export default {
  SummaryOfChatPoint: tStringConstructor<{ text: string }>(`
    Create a summary using less than 10 words of the following interaction, capturing
    the main insights and factual information from both the user's prompt and the
    assistant's response. Focus on presenting the essence of the exchange, ensuring
    the summary is balanced, concise, and informative. priority is given to the assistant's response.
    It is critical that the summary not exceed 10 words. IT MUST NOT EXCEED 10 WORDS!!!!
    If your summary is more than 10 words, then revise it.
    ===  
    {{text}}
  `),
}