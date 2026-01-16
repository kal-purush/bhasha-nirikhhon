import { NextFunction, Request, Response, Router } from 'express';
import UserDataController from '../Controllers/UserDataController';

const router = Router();

router.post('/create', (req: Request, res: Response, next: NextFunction) => {
  new UserDataController(req, res, next).create();
});

router.get('/users', (req: Request, res: Response, next: NextFunction) => {
  new UserDataController(req, res, next).getAllUsersData();
});

router.get('/:id', (req: Request, res: Response, next: NextFunction) => {
  new UserDataController(req, res, next).getUserDataById();
});

router.put('/:id', (req: Request, res: Response, next: NextFunction) => {
  new UserDataController(req, res, next).updateUserDataById();
});

router.delete('/:id', (req: Request, res: Response, next: NextFunction) => {
  new UserDataController(req, res, next).deleteUserDataById();
});

export default router;