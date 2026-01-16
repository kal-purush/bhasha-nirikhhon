import { Request, Response } from 'express';

import { GetLastThreeMessagesService } from '../services/GetLastThreeMessagesService';

export class GetLastThreeMessagesController {
  async handle(_: Request, response: Response) {
    const service = new GetLastThreeMessagesService();

    try {
      const messages = await service.execute();

      return response.json(messages);
    } catch (error) {
      return response.status(500).json({
        error: 'Internal server error',
        message: error?.message ?? 'No info provided',
      });
    }
  }
}