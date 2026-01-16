import asyncHandler from 'express-async-handler';
import { Request, Response } from 'express';
import { UserService } from '../services/user.service';
import { UserNavBar } from '../constants/user';
import { CustomSessionData } from '../interfaces/session.interface';

export class UserController {
  private readonly userService: UserService;
  constructor() {
    this.userService = new UserService();
  }

  public getUserProfile = asyncHandler(
    async (req: Request, res: Response) => {
      const user = (req.session as CustomSessionData).user;
      return res.render('user/profile', {
        user, 
        currentSite: UserNavBar.PROFILE,
      });
    },
  );
}