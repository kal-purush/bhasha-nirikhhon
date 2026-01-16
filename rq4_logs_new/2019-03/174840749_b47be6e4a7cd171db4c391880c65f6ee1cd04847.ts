import { inject, injectable } from 'inversify';

import { IAggregateId, IApplicationEvent } from '../application';
import { IAggregate, IAggregateInstance, IDomainEvent } from '../domain';

import { TYPES } from './constants';
import { IAggregateRepository, IEventStore } from './interfaces';

@injectable()
export class AggregateRepository<T> implements IAggregateRepository<T> {
  private _aggregate: IAggregate<T>;
  private _store: IEventStore;

  constructor(
    aggregate: IAggregate<T>,
    @inject(TYPES.EventStore) store: IEventStore
  ) {
    this._aggregate = aggregate;
    this._store = store;
  }

  public async save(
    id: string,
    events: IDomainEvent[],
    expectedVersion: number
  ) {
    const aggregateId = this._getAggregateId(id);
    return this._store.save(aggregateId, events, expectedVersion);
  }

  public async getById(id: string): Promise<IAggregateInstance<T>> {
    const aggregateId = this._getAggregateId(id);
    const events = await this._store.loadEvents(aggregateId);
    return this._createInstance(events);
  }

  private _getAggregateId(id: string): IAggregateId {
    return { id, name: this._aggregate.name };
  }

  private _createInstance(events: IApplicationEvent[]): IAggregateInstance<T> {
    const { state, version } = this._aggregate.rehydrate(events);
    return { state, version, exists: version > 0 };
  }
}