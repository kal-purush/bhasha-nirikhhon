import * as moment from 'moment-timezone';

export interface ConfigForTransformDateAccordingToTimeZone {
  date?: Date;
  dateFormat?: string;
  timeZone?: string;
}

export function parseDateAccordingToTimeZone(config?: ConfigForTransformDateAccordingToTimeZone): string {
  const dateFormat = config && config.dateFormat ? config.dateFormat : 'dddd, MMMM DD YYYY, h:mm:ss a ZZ';
  const userTimeZone = config && config.timeZone ? config.timeZone : moment.tz.guess();

  return (config && config.date ? moment(config.date) : moment())
    .tz(userTimeZone)
    .format(dateFormat);
}