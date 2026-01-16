import { Controller, Get } from '@overnightjs/core';
import logger from '@src/logger';
import { Request, Response } from 'express';
import { Tracker } from '@src/models/tracker';
import { TrackerService } from '@src/services/tracker-service';
import { BaseController } from '@src/controllers/base-controller';

@Controller('trackers')
export class TrackerController extends BaseController<Tracker> {
  private readonly _trackerService = new TrackerService();
  constructor() {
    super();
  }

  @Get('')
  public async getTracker(req: Request, res: Response): Promise<void> {
    try {
      const {
        orderBy,
        orderField,
      }: {
        orderBy?: 'asc' | 'desc';
        orderField?: keyof Tracker;
      } = req.query;
      const trackers = await this._trackerService.getAll(orderBy);
      res.status(200).send(trackers);
    } catch (error) {
      logger.error(error);
      this.sendErrorResponse(res, {
        code: 500,
        message: 'Something went wrong',
      });
    }
  }
}