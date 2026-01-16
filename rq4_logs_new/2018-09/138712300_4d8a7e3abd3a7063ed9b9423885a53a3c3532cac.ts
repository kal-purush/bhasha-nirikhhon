import { Document, Model } from 'mongoose';
import { BaseRepository } from './base.repository';

export abstract class MongoRepository<T extends Document>
  implements BaseRepository<T> {
  private _model: Model<T>;

  constructor(model: Model<T>) {
    this._model = model;
  }

  public findAll = async (): Promise<T[]> => await this._model.find({});

  public findById = async (id: number): Promise<T> =>
    await this._model.findOne({ _id: id });

  public create = async (data: T) => await this._model.create(data);

  public delete = async (id: number) =>
    await this._model.deleteOne({ _id: id });

  public edit = async (id: number, data: T) =>
    await this._model.updateOne({ _id: id }, data);
}