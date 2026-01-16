import { Injectable, NestMiddleware } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Log, LogDocument } from './schema/log.schema';
import { Request, Response, NextFunction } from 'express';

@Injectable()
export class LogService implements NestMiddleware {
  constructor(@InjectModel(Log.name) private logModel: Model<LogDocument>) {}

  async use(req: Request, res: Response, next: NextFunction) {
    const start = Date.now();
    res.on('finish', async () => {
      const duration = (Date.now() - start) /1000;
      const log = new this.logModel({
        route: req.originalUrl,
        method: req.method,
        responseTime: `${duration} segundos`,
      });
      await log.save();
    });
    next();
  }

  async getLog(): Promise<Log[]>{
    return await this.logModel.find();
  }
}