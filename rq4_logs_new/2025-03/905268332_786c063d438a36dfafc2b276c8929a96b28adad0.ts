import { Request, Response, Router } from 'express';
import UserController from '../controllers/UserController';
import { authToken, CustomRequest } from '../middlewares/authToken';

export class UserRoutes {
  private router: Router;
  private controller: UserController;

  public static build() {
    return new UserRoutes();
  }

  constructor() {
    this.router = Router();
    this.controller = UserController.build();
    this.initializeRoutes();
  }
  private initializeRoutes(): void {
    this.router.use(authToken);

    this.router.get('/:id', (req: CustomRequest, res: Response) => {
      this.controller.find(req, res);
    });

    this.router.get('/:id', (req: Request, res: Response) => {
      this.controller.find(req, res);
    });

    this.router.put('/:id', (req, res) => {
      this.controller.updatePassword(req, res);
    });

    this.router.delete('/:id', (req, res) => {
      this.controller.delete(req, res);
    });
  }

  public getRouter(): Router {
    return this.router;
  }
}