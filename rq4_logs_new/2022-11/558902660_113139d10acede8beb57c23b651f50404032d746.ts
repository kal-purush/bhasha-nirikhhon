import { Alarm } from './alarm';
import { DayOfWeekVO } from './value-objects/dayOfWeek.vo';
import { IdVO } from './value-objects/id.vo';
import { TimeVO } from './value-objects/time.vo';

export class AlarmFactory {
  create(
    id: string,
    dayOfWeek: number,
    hours: number,
    minutes: number
  ): Alarm | null {
    try {
      if (
        id === null ||
        dayOfWeek === null ||
        hours === null ||
        minutes === null
      ) {
        throw new Error('Invalid alarm');
      }

      const idVO = IdVO.create(id);
      const dayOfWeekVO = DayOfWeekVO.create(dayOfWeek);
      const timeVO = TimeVO.create(hours, minutes);

      /* if (dayOfWeek < 0 || dayOfWeek > 6) {
        throw new Error('dayOfWeek must be between 0 and 6');
      } */

      /* if (hours < 8 || hours > 16) {
        throw new Error('hours must be between 8 and 16');
      }

      if (minutes < 0 || minutes > 59) {
        throw new Error('minutes must be between 0 and 59');
      } */

      return new Alarm({
        id: idVO,
        dayOfWeek: dayOfWeekVO,
        time: timeVO,
      });
    } catch (error: any) {
      console.error(`Failed to create alarm: ${error.message}`);
      return null;
    }
  }
}