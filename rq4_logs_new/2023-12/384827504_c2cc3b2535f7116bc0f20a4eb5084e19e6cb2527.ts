import {
  Injectable,
  InternalServerErrorException,
  LoggerService,
} from '@nestjs/common';
import { writeFileSync, existsSync, mkdirSync } from 'fs';

type TLogLevels = 'log' | 'error' | 'warn' | 'debug';

type TLoggerMessage = {
  source?: string;
  level?: TLogLevels;
  message: string;
  value?: Record<string, any> | string;
  fileName?: string;
  method?: string;
  date?: string;
};

@Injectable()
export class SharedLoggerService implements LoggerService {
  public set setLoggerFileName(name: string) {
    this.loggerMessage = { ...this.loggerMessage, fileName: name };
  }

  public set serviceName(name: string) {
    this._serviceName = name;
  }

  public get serviceName(): string {
    return this._serviceName;
  }

  private date = new Date();
  private loggerPath = `${__dirname.replace('/dist', '')}/Logs`;
  private fileName = '';
  private _serviceName = '';
  private fullPath = '';
  private loggerMessage: TLoggerMessage = {
    source: this._serviceName,
    level: 'log',
    message: '',
    fileName: '',
    date: this.createLogDate(false),
  };

  constructor(serviceName: string) {
    this._serviceName = serviceName;
    this.loggerMessage = { ...this.loggerMessage, source: serviceName };
    this.fileName = this.setFileName();
    this.fullPath = this.setFullPath();
    this.createLoggerDir();
  }

  public logInfo({ message, value, fileName, method }: TLoggerMessage) {
    this.handleSetLogLevel({ message, value, fileName, method, level: 'log' });
  }

  public logWarn({ message, value, fileName, method }: TLoggerMessage) {
    this.handleSetLogLevel({ message, value, fileName, method, level: 'warn' });
  }

  public logDebug({ message, value, fileName, method }: TLoggerMessage) {
    this.handleSetLogLevel({
      message,
      value,
      fileName,
      method,
      level: 'debug',
    });
  }

  public logError({ message, value, fileName, method }: TLoggerMessage) {
    this.handleSetLogLevel({
      message,
      value,
      fileName,
      method,
      level: 'error',
    });
  }

  log(message: TLoggerMessage) {
    console.log(`\x1b[32m ${JSON.stringify(message)}\x1b[0m`);
    this.appendToFile(message);
  }

  error(message: TLoggerMessage) {
    console.error(`\x1b[31m ${JSON.stringify(message)}\x1b[0m`);
    this.appendToFile(message);
  }

  warn(message: TLoggerMessage) {
    console.warn(`\x1b[33m ${JSON.stringify(message)}\x1b[0m`);
    this.appendToFile(message);
  }

  debug?(message: TLoggerMessage) {
    console.log(`\x1b[34m ${JSON.stringify(message)}\x1b[0m`);
    this.appendToFile(message);
  }

  private createLogDate(addServiceName: boolean = true): string {
    return `${this.date.getFullYear()}:${this.setMonthPrefix()}:${this.date.getDate()}:${this.date.getHours()}:${this.date.getMinutes()}:${this.date.getSeconds()}:${this.date.getMilliseconds()}${
      addServiceName ? `:${this._serviceName}` : ''
    }`;
  }

  private setMonthPrefix(): string {
    if (this.date.getMonth() < 10) {
      return `0${this.date.getMonth()}`;
    }
    return `${this.date.getMonth()}`;
  }

  private appendToFile(message: any) {
    if (this.checkIfFileOrDirectoryExists(this.fullPath)) {
      try {
        writeFileSync(this.fullPath, message + '\n', {
          flag: 'a',
          encoding: 'utf-8',
        });
      } catch (error) {
        throw new InternalServerErrorException(
          error,
          'Error creating or writing to file',
        );
      }
    }
  }

  private checkIfFileOrDirectoryExists(path: string): boolean {
    return existsSync(path);
  }

  private createLogFile(fullPath: string, fileName: string): string {
    const splitFileNameDay = fileName.split('-')[2].replace('.', '');
    const day = parseInt(splitFileNameDay);
    if (this.date.getDate() !== day) {
      this.fileName = this.setFileName();
      this.fullPath = this.setFullPath();
      return this.fullPath;
    }
    return fullPath;
  }

  private callWriteFile() {
    try {
      writeFileSync(
        this.createLogFile(this.fullPath, this.fileName),
        `${this.createLogDate()} Logger Started...\n`,
        {
          encoding: 'utf-8',
        },
      );
    } catch (error) {
      throw new InternalServerErrorException(
        error,
        'Error creating or writing to file',
      );
    }
  }

  private createLoggerDir() {
    if (this.checkIfFileOrDirectoryExists(this.fullPath)) {
      // this.log('Restarting Nest Application...');
      return;
    } else if (!this.checkIfFileOrDirectoryExists(this.loggerPath)) {
      try {
        mkdirSync(this.loggerPath, { recursive: true });
        this.callWriteFile();
      } catch (error) {
        console.error(`error creating directory:${this.loggerPath}:${error}`);
        return;
      }
    } else if (this.checkIfFileOrDirectoryExists(this.loggerPath)) {
      this.callWriteFile();
    }
  }

  private setFileName() {
    return `${this.date.getFullYear()}-${this.setMonthPrefix()}-${this.date.getDate()}.log`;
  }

  private setFullPath() {
    return this.loggerPath + '/' + this.fileName;
  }

  private handleSetLogLevel({
    message,
    value,
    fileName,
    method,
    level,
  }: TLoggerMessage) {
    if (value && typeof value === 'string') {
      this.loggerMessage = {
        ...this.loggerMessage,
        level,
        message,
        value,
        fileName,
        method,
      };
      this[level](this.loggerMessage);
    } else if (value && typeof value !== 'string') {
      this.loggerMessage = {
        ...this.loggerMessage,
        level,
        value,
        message,
        fileName,
        method,
      };
      this[level](this.loggerMessage);
    } else if (!value) {
      this.loggerMessage = {
        ...this.loggerMessage,
        level,
        message,
        fileName,
        method,
      };
      this[level](this.loggerMessage);
    }
  }
}