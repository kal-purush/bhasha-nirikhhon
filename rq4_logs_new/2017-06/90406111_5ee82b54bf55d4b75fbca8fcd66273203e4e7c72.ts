import {Repository} from '@loopback/data';

export class TodoRepo extends Repository {
  constructor(private @inject('models.todo') model, private @inject('datasources.db') db) {
    super(model, db)
  }
}