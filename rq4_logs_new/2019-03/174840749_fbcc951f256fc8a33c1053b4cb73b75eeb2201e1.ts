import { IAggregate } from '../domain';
import { createAggregateRepository, IAggregateRepository } from '../infrastructure/aggregateRepository';
import { createEventStore, IEventStore, IStorageDriverOpts } from '../infrastructure/eventstore';

import { IApplicationCommand } from './Command';
import { IApplicationEvent } from './Event';


export type ICommandHandler = (command: IApplicationCommand) => Promise<IApplicationEvent[] | void>;

interface IExplicitEventStoreOptions {
  store: IEventStore
}

interface ICommandHandlerFactoryOptions {
  eventStore: IExplicitEventStoreOptions | IStorageDriverOpts
}

function isExplicitEventStore(opts: IExplicitEventStoreOptions | IStorageDriverOpts): opts is IExplicitEventStoreOptions {
  return (opts as IExplicitEventStoreOptions).hasOwnProperty('store');
}

async function processCommand<T>(aggregate: IAggregate<T>, repository: IAggregateRepository<T>, command: IApplicationCommand) {
  const { aggregate: { id, name } } = command;
  const entity = await repository.getById(id!);

  const wrapReject = (cmd: IApplicationCommand): IApplicationCommand => {
    const { reject: actualReject, ...rest } = cmd;

    const reject = (reason: string) => {
      console.log(
        `Aggregate ${name}: ${id} rejected "${command.name}" command: ${reason}`
      );
      actualReject(reason);
    };

    return { ...rest, reject };
  };

  const events = aggregate.applyCommand(entity, wrapReject(command));

  if (events.length) {
    return repository.save(id!, entity.version, events);
  }
  return Promise.resolve();
}

export const commandHandlerFactory = ({ eventStore }: ICommandHandlerFactoryOptions) => {
  const store = isExplicitEventStore(eventStore) ? eventStore.store : createEventStore(eventStore);

  function getCommandHandler<T>(aggregate: IAggregate<T>) {
    const repository = createAggregateRepository<T>(aggregate, store);

    const handleCommand = async (command: IApplicationCommand) => processCommand(aggregate, repository, command);

    return handleCommand
  }

  return getCommandHandler
}