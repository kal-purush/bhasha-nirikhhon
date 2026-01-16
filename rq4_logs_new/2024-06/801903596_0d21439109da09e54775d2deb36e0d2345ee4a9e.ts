import { OrderService } from './../../services/order.service';
import asyncHandler from 'express-async-handler';
import {Request, Response} from 'express';
import { CustomSessionData } from '../../interfaces/session.interface';
import { plainToClass } from 'class-transformer';
import { validate } from 'class-validator';
import { formatDate, handleError } from '../../commons/utils';
import { AdminNavBar } from '../../constants/admin';
import { UserService } from '../../services/user.service';
import { UserPageOptions } from '../../commons/dtos/userPageOptions.dto';
import { UserEntity } from '../../entities/user.entity';

export class AdminUserController {
  private readonly userService: UserService;
  private readonly orderService: OrderService;
  constructor(){
    this.userService = new UserService();
    this.orderService = new OrderService();
  }

  public getAdminUserPage = asyncHandler(
    async (req: Request, res: Response) => {
      const admin = (req.session as CustomSessionData).admin;
      const pageOptions = plainToClass(UserPageOptions, req.query);
      const rawErrors = await validate(pageOptions);
      if( rawErrors.length > 0 ){
        const errors = handleError(rawErrors, req, res);
        return res.render('admin/user/list', {
          admin,
          currentSite: AdminNavBar.USER,
          errors, 
          query: pageOptions,
          formatDate,
        });
      }
      const userPage = await this.userService.getUserPage(pageOptions);
      return res.render('admin/user/list', {
        admin,
        currentSite: AdminNavBar.USER,
        users: userPage.data, 
        meta: userPage.meta, 
        query: pageOptions,
        formatDate,
      });
    },
  );

  private getUserFromIdParam = async (
    req: Request,
    res: Response,
  ): Promise<UserEntity | null> => {
    const id = parseInt(req.params.id);
    if (isNaN(id)) {
      req.flash('error', 'error.invalidParameter');
      res.redirect('/admin/user');
    }
    return await this.userService.findById(id);
  };

  public adminGetUserDetail = asyncHandler(
    async (req: Request, res: Response) => {
      const admin = (req.session as CustomSessionData).admin;
      const user = await this.getUserFromIdParam(req, res);
      if(user === null) {
        return res.redirect('/admin/user');
      }
      const userTotalOrder = 
      await this.orderService.getUserOrderNumber(user.id);
      return res.render('admin/user/detail', {
        admin,
        currentSite: AdminNavBar.USER,
        user,
        userTotalOrder,
        formatDate,
      });
    },
  );
}