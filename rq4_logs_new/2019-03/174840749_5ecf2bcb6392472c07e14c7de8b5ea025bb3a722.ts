export type IRejectFunction = (reason: string) => void;

export interface IDomainCommand {
  // Name of the command
  name: string;

  // Parameters associated with this command
  data?: any;

  // User initiating the command
  user?: object;

  // Hook to reject the command if it isn't valid
  reject: (reason: string) => void;
}

export interface IDomainEvent {
  // Name of this event
  name: string;

  // Data associated with this event
  data?: any;
}

export interface IDomainService {
  [s: string]: (...args: any) => Promise<any>;
}

export interface IServiceRegistry {
  [s: string]: IDomainService;
}

export interface IVersionedEntity<T> {
  // Current state of the entity
  state: T;

  update?: (state: T) => IVersionedEntity<T>;

  // Current version of the entity
  version: number;
}

export interface IAggregateInstance<T> {
  // Boolean value indicating whether this instance already exists or is not yet created
  exists: boolean;

  // Current state of the aggregate
  state: T;

  // Current version of the aggregate
  version: number;
}

export interface IEventHandlerMap<T> {
  [s: string]: (state: T, event: IDomainEvent) => T;
}

type CommandHandlerResult = IDomainEvent | IDomainEvent[] | void;

export interface ICommandHandlerMap<T> {
  [s: string]: (
    entity: IAggregateInstance<T>,
    command: IDomainCommand,
    services?: IServiceRegistry
  ) => CommandHandlerResult | Promise<CommandHandlerResult>;
}

export interface IAggregateDefinition<T> {
  name: string;

  initialState: T;

  getNextId?: () => string;

  eventHandlers: IEventHandlerMap<T>;

  commands: ICommandHandlerMap<T>;
}

export interface IAggregate<T> {
  readonly name: string;
  rehydrate: (
    events: IDomainEvent[],
    snapshot?: IVersionedEntity<T>
  ) => IVersionedEntity<T>;
  applyCommand: (
    entity: IAggregateInstance<T>,
    command: IDomainCommand,
    services: IServiceRegistry
  ) => Promise<IDomainEvent[]>;
}