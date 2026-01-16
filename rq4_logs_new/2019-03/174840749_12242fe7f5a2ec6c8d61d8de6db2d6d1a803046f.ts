import { createEvent } from './Event';
import {
  IAggregateDefinition,
  IAggregateRoot,
  IAggregateState,
  IDomainCommand,
  IDomainEvent,
  IPublishableAggregateState,
  IServiceRegistry
} from './interfaces';

export function createAggregateRoot<T>(
  definition: IAggregateDefinition<T>
): IAggregateRoot<T> {
  const { name: aggregateName, reducer: eventHandlers, commands } = definition;

  const initialState: IAggregateState<T> = {
    exists: false,
    state: definition.initialState,
    version: 0
  };

  const applyEvent = (
    aggregate: IAggregateState<T>,
    event: IDomainEvent
  ): IAggregateState<T> => {
    const { name } = event;
    const update = eventHandlers[name];
    const state = update(aggregate.state, event);
    return {
      state,
      exists: aggregate.version !== 0,
      version: aggregate.version + 1
    };
  };

  const handle = async (
    entity: IAggregateState<T>,
    command: IDomainCommand,
    services?: IServiceRegistry
  ): Promise<IDomainEvent[]> => {
    const handler = commands[command.name];

    const events: IDomainEvent[] = [];
    let instance: IPublishableAggregateState<T>;

    const publish = (
      name: string,
      data?: object
    ): IPublishableAggregateState<T> => {
      const e = createEvent(name, data);
      events.push(e);
      instance = { ...applyEvent(instance, e), publish };
      return instance;
    };

    instance = { ...entity, publish };

    const generator = handler({ ...entity, publish }, command, services);

    // Handler method can either call `entity.publish` directly & return void,
    // or it can be a generator, in which case we need to iterate through all
    // the published events
    if (generator !== undefined) {
      let event = await Promise.resolve(generator.next(instance));

      while (!event.done) {
        event = await Promise.resolve(generator.next(instance));
      }
    }

    return Promise.resolve(events);
  };

  function rehydrate(events: IDomainEvent[], snapshot?: IAggregateState<T>) {
    return events.reduce(applyEvent, snapshot || initialState);
  }

  return {
    applyEvent,
    initialState,
    rehydrate,
    applyCommand: handle,
    name: aggregateName
  };
}