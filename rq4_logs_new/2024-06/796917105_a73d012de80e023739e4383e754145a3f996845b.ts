import { Injectable } from '@nestjs/common';
import { OpenAIService } from './openai.service';

@Injectable()
export class OpenAIClientService {
  constructor(private readonly openAIService: OpenAIService) {}

  async getCompletion(prompt: string): Promise<string> {
    const openai = this.openAIService.getOpenAIClient();
    const completion = await openai.chat.completions.create({
      messages: [{ role: "system", content: prompt }],
      model: "gpt-3.5-turbo",
    });

    return completion.choices[0].message.content.trim();
  }
}