import uuid from 'uuid';

import { createCommand as createDomainCommand, IDomainCommand, IRejectFunction } from '../domain'
import { IAggregateId } from './interfaces';

export interface IApplicationCommand extends IDomainCommand {
  // Unique identifier for this command
  id: string | number;

  // Aggregate that should receive this command
  aggregate: IAggregateId;

  // Version of the aggregate at the time this command was initiated
  version: number;
}

export function createCommand(aggregate: IAggregateId, version: number, name: string, reject: IRejectFunction, data?: object, id?: string | number, user?: object): IApplicationCommand {
  const domainCommand = createDomainCommand(name, reject, data);

  return {
    ...domainCommand,
    aggregate,
    user,
    version,
    id: id || uuid.v4()    
  }
}