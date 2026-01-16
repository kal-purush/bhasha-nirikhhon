import { Request, Response } from 'express';

import { CreateMessageService } from '../services/CreateMessageService';

export class CreateMessageController {
  async handle(request: Request, response: Response) {
    const { user_id } = request;
    const { text } = request.body;

    const service = new CreateMessageService();

    try {
      const message = await service.execute({ text, user_id });

      return response.status(201).json(message);
    } catch (error) {
      return response.status(500).json({
        error: 'Internal server error',
        message: error?.message ?? 'No info provided',
      });
    }
  }
}