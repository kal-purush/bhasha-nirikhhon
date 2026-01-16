import { openai } from '../../config/openAI';

export const getOpenAIRecommendation = async (watchedContentNames: string[]): Promise<string[]> => {
  let chatCompletionContent = "Based on these series and movies in array of strings format below, give me 10 series and movies recommendation in array format, don't write anything else, just the array:";
    chatCompletionContent = `${chatCompletionContent} [${watchedContentNames}]`;
  
    const completion = await openai.createChatCompletion({
      model: "gpt-3.5-turbo",
      messages: [{role: "user", content: chatCompletionContent}],
    });
    
    return JSON.parse(completion.data.choices[0].message?.content ?? '') as string[];
}