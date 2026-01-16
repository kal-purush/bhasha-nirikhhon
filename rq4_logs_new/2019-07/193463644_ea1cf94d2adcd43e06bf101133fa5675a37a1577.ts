import * as morgan from 'morgan';
import * as fs from 'fs';
import * as path from 'path';
import * as moment from 'moment-timezone';

export class Logger {
  infoLogStream: fs.WriteStream;
  errorLogStream: fs.WriteStream;
  constructor() {
    this.infoLogStream = fs.createWriteStream(path.join('.', 'logs', 'info.log'), {
      flags: 'a'
    });
    this.errorLogStream = fs.createWriteStream(path.join('.', 'logs', 'error.log'), {
      flags: 'a'
    });

    morgan.token('date', () => {
      return moment.tz('Europe/Helsinki').format();
    });

    morgan.format(
      'outFormat',
      ':remote-addr [:date] :method [:url] HTTP/:http-version [:status] :res[content-length] - :response-time ms [:user-agent]'
    );

    morgan.format(
      'incFormat',
      ':remote-addr [:date] :method [:url] HTTP/:http-version [:user-agent]'
    );
  }
}