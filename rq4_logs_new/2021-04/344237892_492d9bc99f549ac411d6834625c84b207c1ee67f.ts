import { Message } from 'kafkajs';
import { IErrorService } from '../../../resources/errors/interfaces/errorService.interface';

export abstract class Handler {
  protected errorService: IErrorService;

  constructor(errorService: IErrorService) {
    this.errorService = errorService;
  }

  protected checkVersion(version: string, message: Message) {
    const { event_version } = message.headers;
    return version === event_version.toString();
  }

  protected async createError(message: Message) {
    const error = await this.errorService.create(message);
    throw new Error(`Unsupported version. Error id: ${error.id}`);
  }

  public abstract consumeMessage(message: Message): Promise<void>;
}