import { BaseMigration, Database } from '@ubiquits/core/server';
import { Migration, Logger } from '@ubiquits/core/common';

/**
 * Note!
 * This migration is implemented as a quick-and-dirty demo to get you up and running asap
 * @todo(zh) when full migration capabilities are implemented refactor this class
 */
@Migration()
export class AllTablesMigration extends BaseMigration {

  constructor(logger:Logger, database:Database){
    super(logger, database);
  }

  public migrate(): Promise<void> {
    return this.database.getConnection().then((connection:any) => {
      return connection.syncSchema(true);
    });

  }

}