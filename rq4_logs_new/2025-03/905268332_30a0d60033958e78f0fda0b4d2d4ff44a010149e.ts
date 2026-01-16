import { Request, Response } from 'express';
import { prisma } from '../../../utils/prisma';
import { UserRepository } from '../../../repositories/UserRepository';
import { AuthService } from '../../../services/AuthService';

export class AuthController {
  private constructor() {}

  public static build() {
    return new AuthController();
  }

  public async register(req: Request, res: Response) {
    try {
      const { name, email, password } = req.body;

      const repository = UserRepository.build(prisma);
      const service = AuthService.build(repository);

      const output = await service.register(name, email, password);
      const data = {
        id: output.id,
        name: output.name,
        email: output.email,
      };
      return res.status(201).json(data);
    } catch (error) {
      if (error instanceof Error)
        return res.status(400).json({ error: error.message });
    }
  }

  public async loginWithEmailAndPassword(req: Request, res: Response) {
    try {
      const { email, password } = req.body;

      const repository = UserRepository.build(prisma);
      const service = AuthService.build(repository);

      const output = await service.loginWithEmailAndPassword(email, password);
      const data = {
        id: output.id,
        email: output.email,
        token: output.token,
      };
      return res.status(200).json(data);
    } catch (error) {
      if (error instanceof Error)
        return res.status(400).json({ error: error.message });
    }
  }
}