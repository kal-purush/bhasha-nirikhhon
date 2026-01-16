import { Request, Response } from 'express';

import { GetUserProfileService } from '../services/GetUserProfileService';

export class GetUserProfileController {
  async handle(request: Request, response: Response) {
    const { user_id } = request;

    const service = new GetUserProfileService();

    try {
      const profile = await service.execute({ user_id });

      return response.json(profile);
    } catch (error) {
      return response.status(500).json({
        error: 'Internal server error',
        message: error?.message ?? 'No info provided',
      });
    }
  }
}