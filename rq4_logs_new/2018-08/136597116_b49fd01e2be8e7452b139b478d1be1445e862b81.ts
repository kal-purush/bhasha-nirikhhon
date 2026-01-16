import { ILogger, IEntryMeta } from './types'

export class LoggerMock implements ILogger {
  public debug(message: string, meta?: IEntryMeta): void {
    return undefined
  }
  public info(message: string, meta?: IEntryMeta): void {
    return undefined
  }
  public warning(message: string, meta?: IEntryMeta): void {
    return undefined
  }
  public error(message: string, meta?: IEntryMeta): void {
    return undefined
  }
  public eventError(entity: string, method: string, meta?: IEntryMeta): void {
    return undefined
  }
  public eventInfo(entity: string, method: string, meta?: IEntryMeta): void {
    return undefined
  }
}