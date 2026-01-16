import 'reflect-metadata';
import { ObservableBus, ICommand, ICommandBus, ICommandHandler, CommandHandlerNotFoundException, CommandHandlerMetatype, InvalidModuleRefException, InvalidCommandHandlerException } from '@nestjs/cqrs';
import { COMMAND_HANDLER_METADATA } from '@nestjs/cqrs/src/utils/constants';

/**
 * Commandbus with broader visibility for inheritance.
 * @template H - Handler type.
 * @see https://github.com/nestjs/cqrs/blob/master/src/command-bus.ts
 */
export abstract class AbstractCommandBus<H> extends ObservableBus<ICommand> implements ICommandBus {
  protected handlers = new Map<string, H>();
  protected moduleRef = null;

  setModuleRef(moduleRef) {
    this.moduleRef = moduleRef;
  }

  abstract execute<T extends ICommand>(command: T): Promise<any>;

  bind(handler: H, name: string) {
    this.handlers.set(name, handler);
  }

  register(handlers: CommandHandlerMetatype[]) {
    handlers.forEach(handler => this.registerHandler(handler));
  }

  protected registerHandler(handler: CommandHandlerMetatype) {
    if (!this.moduleRef) {
      throw new InvalidModuleRefException();
    }
    const instance = this.moduleRef.get(handler);
    if (!instance) return;

    const target = this.reflectCommandName(handler);
    if (!target) {
      throw new InvalidCommandHandlerException();
    }
    this.bind(instance as H, target.name);
  }

  protected getCommandName(command): string {
    const { constructor } = Object.getPrototypeOf(command);
    return constructor.name as string;
  }

  private reflectCommandName(
    handler: CommandHandlerMetatype,
  ): FunctionConstructor {
    return Reflect.getMetadata(COMMAND_HANDLER_METADATA, handler);
  }
}