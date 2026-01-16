import { Document, FilterQuery, Model, UpdateQuery } from 'mongoose';

export default abstract class EntityRepository<T extends Document> {
  constructor(private readonly EntityModel: Model<T>) {}

  public async findOne(entityFilterQuery: FilterQuery<T>): Promise<T | null> {
    return await this.EntityModel.findOne(entityFilterQuery).exec();
  }

  public async findById(entityId: string): Promise<T | null> {
    return await this.EntityModel.findById(entityId).exec();
  }

  public async find(entityFilterQuery: FilterQuery<T>): Promise<T[]> {
    return await this.EntityModel.find(entityFilterQuery).exec();
  }

  public async create(createEntityData: unknown): Promise<T> {
    const entity = new this.EntityModel(createEntityData);
    return await this.save(entity);
  }

  public async findOneAndUpdate(
    entityFilterQuery: FilterQuery<T>,
    updateEntityData: UpdateQuery<unknown>,
  ): Promise<T | null> {
    return await this.EntityModel.findOneAndUpdate(entityFilterQuery, updateEntityData, { new: true }).exec();
  }

  public async deleteMany(entityFilterQuery: FilterQuery<T>): Promise<boolean> {
    const result = await this.EntityModel.deleteMany(entityFilterQuery).exec();
    return result.deletedCount > 0;
  }

  public async save(entity: T): Promise<T> {
    return await entity.save();
  }
}