import { Request, Response, Router } from 'express';
import { AuthController } from '../controllers/AuthController';
import { validateData } from '../middlewares/validateData';
import {
  authLoginSchema,
  authRegistrationSchema,
} from '../../../schemas/authSchemas';

export class AuthRoutes {
  private router: Router;
  private controller: AuthController;

  constructor() {
    this.router = Router();
    this.controller = AuthController.build();
    this.initializeRoutes();
  }

  public static build() {
    return new AuthRoutes();
  }

  private initializeRoutes(): void {
    this.router.post(
      '/register',
      validateData(authRegistrationSchema),
      (req: Request, res: Response) => {
        this.controller.register(req, res);
      },
    );

    this.router.post(
      '/login',
      validateData(authLoginSchema),
      (req: Request, res: Response) => {
        this.controller.loginWithEmailAndPassword(req, res);
      },
    );
  }

  public getRouter(): Router {
    return this.router;
  }
}