import { Request, Response } from 'express';

import { AuthenticateUserWithGithubService } from '../services/AuthenticateUserWithGithubService';

export class AuthenticateUserWithGithubController {
  async handle(request: Request, response: Response): Promise<Response> {
    const { code } = request.body;

    const service = new AuthenticateUserWithGithubService();

    try {
      const result = await service.execute({ code });

      return response.json(result);
    } catch (error) {
      console.log(error);

      return response.status(500).json({
        error: 'Internal server error',
        message: error?.message ?? 'No info provided',
      });
    }
  }
}