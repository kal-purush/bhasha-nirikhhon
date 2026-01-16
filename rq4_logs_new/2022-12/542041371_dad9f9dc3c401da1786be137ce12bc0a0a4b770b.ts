import type { NextFunction, Request, Response } from 'express';
import ApiError from '../utils/error';
import { SiteTheme } from '../models/theme.model';
import User from '../models/user.model';
import { SITE_THEMES } from '../utils/const';

class ThemeController {
  async getAll(_req: Request, res: Response, next: NextFunction) {
    try {
      const themes = await SiteTheme.findAll();
      return res.json(themes);
    } catch (error: any) {
      return next(ApiError.badRequest(error.message));
    }
  }

  async set(req: Request, res: Response, next: NextFunction) {
    try {
      const { themeName } = req.query;
      const exist = SITE_THEMES.some(theme => theme.theme === themeName);
      if (!themeName || typeof themeName !== 'string' || !exist) {
        return next(
          ApiError.badRequest('Не передано имя темы или тема не найдена')
        );
      }
      const { userId } = req.user;
      const user = await User.findByPk(userId);
      if (!user) {
        return next(ApiError.badRequest('Пользователь не найден'));
      }
      user.theme = themeName;
      await user.save();
      return res.status(200).json({ theme: user.theme });
    } catch (error: any) {
      return next(ApiError.badRequest(error.message));
    }
  }
}

export default new ThemeController();