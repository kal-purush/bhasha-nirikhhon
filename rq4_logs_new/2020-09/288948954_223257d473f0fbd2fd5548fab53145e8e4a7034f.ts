import { Request, Response } from 'express';

import db from '../database/connection';
import convertHourToMinutes from '../utils/convertHourToMinutes';

interface ScheduleItem {
  weekday: number;
  from: string;
  to: string;
}

export default class ClassesController {
  async index(req: Request, res: Response) {
    const filters = req.query;

    const weekday = filters.weekday as string;
    const subject = filters.subject as string;
    const time = filters.time as string;

    if (!filters.weekday || !filters.subject || !filters.time) {
      return res.status(400).json({
        error: 'Missing filters to search classes',
      });
    }

    const timeToMinutes = convertHourToMinutes(time);

    const classes = await db('classes')
      .whereExists(function () {
        this.select('class_schedule.*')
          .from('class_schedule')
          .whereRaw('`class_schedule`.`class_id` = `classes`.`id`')
          .whereRaw('`class_schedule`.`weekday` = ??', [Number(weekday)])
          .whereRaw('`class_schedule`.`from` <= ??', [timeToMinutes])
          .whereRaw('`class_schedule`.`to` > ??', [timeToMinutes])
      })
      .where('classes.subject', '=', subject)
      .join('users', 'classes.user_id', '=', 'users.id')
      .select('classes.*', 'users.*');

    return res.json(classes);
  }

  async create(req: Request, res: Response) {
    const { name, avatar, whatsapp, bio, subject, cost, schedule } = req.body;

    const trx = await db.transaction();

    try {
      const insertetUserIds = await trx('users').insert({
        name,
        avatar,
        whatsapp,
        bio,
      });

      const insertedClassesIds = await trx('classes').insert({
        user_id: insertetUserIds[0],
        subject,
        cost,
      });

      const classSchedule = schedule.map((scheduleItem: ScheduleItem) => {
        return {
          class_id: insertedClassesIds[0],
          weekday: scheduleItem.weekday,
          from: convertHourToMinutes(scheduleItem.from),
          to: convertHourToMinutes(scheduleItem.to),
        };
      });

      await trx('class_schedule').insert(classSchedule);

      await trx.commit();

      return res.status(201).send();
    } catch (error) {
      await trx.rollback();

      return res.status(400).json({
        error: 'Unexpected error while creating new class',
      });
    }
  }
}