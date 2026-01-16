import * as express from 'express';
import Controller from '../../interfaces/controller.interface';

class AuthenticationController implements Controller {
  public path = '/auth';
  public router = express.Router();

  constructor() {
    this.initializeRoutes();
  }

  private initializeRoutes() {
    this.router.post(`${this.path}/register`, this.registration);
  }

  private registration = async (request: express.Request, response: express.Response, next: express.NextFunction) => {
    

    response.send("Alive");
}
}

export default AuthenticationController;