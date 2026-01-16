import { NextFunction, Request, Response } from 'express';
import { sendEmail } from '../services/email.service';
import path from 'path';
import { generateRandomDigitsCode, readFile } from '../utils/functions.utils';
import RedisCache from '../services/redis.service';
import { AppError } from '../utils/AppError';
import { userGetOrCreate } from '../services/user.service';

export const sendCode = async (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  try {
    const code = generateRandomDigitsCode(6);

    await RedisCache.setKeyWithValue({
      key: req.body.email,
      value: JSON.stringify({ code, name: req.body.name }),
      expirationTime: 60 * 5,
    });

    const pathToFile = path.join(
      __dirname,
      '..',
      '..',
      'public',
      'verify-code.html'
    );

    const html = (await readFile(pathToFile)).replace('**XXXXXX**', code);

    const sendEmailres = await sendEmail({
      to: req.body.email,
      subject: 'test',
      text: `test`,
      html: html,
    });

    res.status(sendEmailres[0].statusCode || 202).json({
      message: 'code sent successfully',
    });
  } catch (error) {
    next(error);
  }
};

export const verifyCode = async (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  try {
    const { email, code } = req.body;

    const redisResult = await RedisCache.getValueByKey<string>(email);

    if (!redisResult) {
      throw new AppError('AppError', "code doesn't exist", 500, 'Redis');
    }

    const { code: redisCode, name } = JSON.parse(redisResult);

    if (redisCode !== code) {
      throw new AppError('AppError', 'wrong code', 401, 'Redis');
    }

    const user = await userGetOrCreate({
      email,
      name,
    });

    res.status(200).json(user);
  } catch (error) {
    next(error);
  }
};