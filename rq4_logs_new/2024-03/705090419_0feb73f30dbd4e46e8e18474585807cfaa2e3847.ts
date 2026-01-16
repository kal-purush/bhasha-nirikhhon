// openaiService.ts
import { OpenAI } from "openai";

// Function to initialize the OpenAI client
export const initializeOpenAI = (openaiKey: string) => {
  return new OpenAI({
    apiKey: openaiKey,
    dangerouslyAllowBrowser: true,
  });
};

// Function to generate a response with a delay
export const generateResponseWithDelay = async (
  openai: OpenAI,
  chunks: string[],
  conversationContext: any[][]
): Promise<string> => {
  const transcribedText = chunks.join(" ");
  const prompt = `
    Transcribed Text:
    ${transcribedText}
    Convert the above transcribed text into key points and main ideas using Markdown, which splits words and text into headers/headings, bullet points, bolding, italics, underlines, etc. (and any combination thereof)? Ensure YOU DO NOT deviate from this style format for every message given to you. Sometimes, the message will not be long enough, and you may need to wait a bit before processing the file. DO NOT just convert text to markdown. Highlight what is important information to take out from the provided conversation using a traditional style guide. Create it as if you were writing detailed notes with important examples. Do not miss out on information. Ensure the generated text includes relevant details about the topic discussed. Please additionally add a summary at the end or a conclusion. Adapt the response to the context of the conversation, including concepts, examples, and any recommended style guide. Output generated markdown as a code block. Do not allow the generated text to fall outside the code block.
    `;
  const modelId = "gpt-3.5-turbo-instruct"; // Update with the appropriate model ID

  // Restore the previous context
  let currentMessages = [];
  for (const [inputText, responseText] of conversationContext) {
    currentMessages.push({ role: "user", content: inputText });
    currentMessages.push({ role: "assistant", content: responseText });
  }

  // Add the new user message to the context
  currentMessages.push({ role: "user", content: transcribedText });

  // Call OpenAI API
  const result = await openai.completions.create({
    model: modelId,
    prompt: prompt,
    max_tokens: 500,
  });

  const responseText = result.choices[0].text;
  conversationContext.push([prompt, responseText]);

  return responseText;
};

// Function to generate a response without delay
export const generateResponse = async (
  openai: OpenAI,
  chunks: string[],
  conversationContext: any[][]
): Promise<string> => {
  const response = await generateResponseWithDelay(openai, chunks, conversationContext);
  await new Promise((resolve) => setTimeout(resolve, 1000)); // Delay between requests
  return response;
};