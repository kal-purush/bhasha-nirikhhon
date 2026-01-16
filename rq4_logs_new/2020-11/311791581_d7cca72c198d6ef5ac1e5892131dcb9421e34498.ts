import { Request, Response } from 'express';

import { ImportMovieUseCase } from './ImportMovieUseCase';

export class ImportMovieController {
    private importMovieUseCase: ImportMovieUseCase;

    constructor(importMovieUseCase: ImportMovieUseCase) {
      this.importMovieUseCase = importMovieUseCase;
    }

    public async handle(request: Request, response: Response):Promise<Response> {
      const { id } = request.params;

      try {
        await this.importMovieUseCase.execute(id);
        return response.sendStatus(201).json({ message: 'Movie successfully imported' });
      } catch (error) {
        return response.sendStatus(400).json({ message: error.message || 'Unexpected Error' });
      }
    }
}