import { Controller, Get, Query } from '@nestjs/common';
import { log } from 'console';

import { OpenAIService } from './openai.service';

@Controller('openai')
export class OpenAIController {
  constructor(private readonly OpenAIService: OpenAIService) {}

  @Get('completion')
  async getCompletion(
    @Query('question') question: string,
  ): Promise<string> {
    try {
      const openai = this.OpenAIService.getOpenAIClient();

      const completion = await openai.chat.completions.create({
        messages: [{ role: "system", content: question }],
        model: "gpt-3.5-turbo",
      });

      return completion.choices[0].message.content.trim();
    } catch (error) {
      log('Erro ao enviar request para a OpenAI:', error);

      return 'Tente novamente.';
    }
  }
}