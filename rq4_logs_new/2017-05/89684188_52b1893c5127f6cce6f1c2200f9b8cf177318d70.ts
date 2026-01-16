import * as mongoose from 'mongoose';

import { Pirate, PirateModel } from './pirate';

export class DB {
  constructor() { }

  create(pirate: Pirate): Promise<Pirate> {
    let p = new PirateModel(pirate);
    return p.save();
  }

  read(query: any): mongoose.DocumentQuery<Pirate[], Pirate> {
    return PirateModel.find(query);
  }

  update(pirate: Pirate): mongoose.Query<number> {
    return PirateModel.update({name: pirate.name}, {...pirate});
  }

  delete(pirate: Pirate): mongoose.Query<void> {
    return PirateModel.remove({name: pirate.name});
  }
}