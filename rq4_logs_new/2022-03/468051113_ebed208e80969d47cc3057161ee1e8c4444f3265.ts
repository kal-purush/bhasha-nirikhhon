import { SportsEntity } from '../typeorm/entities/sports.entities';
import { SportsRepository } from '../typeorm/repositories/sports.repositories';
import { SportsDTO } from '../dto/sports.dto';
import { getRepository } from 'typeorm';
import AppError from '@shared/errors/AppError';


export class CreateSports {

  async execute({ name, category }: SportsDTO): Promise<SportsEntity | Error> {
    const repo = getRepository(SportsEntity);

    if (await repo.findOne({ category })) {
      return new Error("Category already exists")
    }
    const sports = repo.create({ name, category });
    return await repo.save(sports);
  }

}