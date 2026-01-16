import { UserStore } from '../../common/stores/user.store';
import { Collection } from '@ubiquits/core/common';
import { DatabaseStore, Database, NotFoundException } from '@ubiquits/core/server';
import { Injectable, Injector } from '@angular/core';
import { User } from '../../common/models/user.model';
import { Logger } from '@ubiquits/core/common';
import { UserMockStore } from '../../common/stores/user.mock.store';
import { UserDatabaseStore } from '../stores/user.db.store';

@Injectable()
export class DemoSeeder {

  protected logger:Logger;

  constructor(loggerBase: Logger, protected userStore: UserStore, protected userMockStore: UserMockStore) {

    this.logger = loggerBase.source('Demo Seeder');
    
  }


  public seed():Promise<void> {
    this.logger.info('Seeding database');
    return this.userStore.initialized()
      .then(() => this.userStore.findOne(process.env.DEMO_ID))
      .then((instance: User) => {

        this.logger.debug('Demo model already seeded', instance);
      })
      .catch((e) => {

        if (e instanceof NotFoundException){
          this.logger.debug('Creating demo models');

          return this.userMockStore.findMany()
            .then((mockModels: Collection<User>) => {

              return this.userMockStore.findOne(process.env.DEMO_ID)
                .then((user: User) => {
                  mockModels.push(user);
                  return (this.userStore as UserDatabaseStore).getRepository().then((repo:any) => repo.persistMany(...mockModels));
                });

            });
        }

        throw e;

      });
  }

}