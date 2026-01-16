import { getRepository } from "typeorm";
import { SportsEntity } from "../typeorm/entities/sports.entities";
import { UpdateSportsDTO } from "../dto/sports.dto";
import AppError from "@shared/errors/AppError";




export class UpdateSports {

  async execute({ id, name, category }: UpdateSportsDTO) {
    const sportsRepository = getRepository(SportsEntity);
    const sportsId = await sportsRepository.findOne(id);
    if (!sportsId) throw new AppError('Sports not found');

    return sportsRepository.update(id, { name, category, update_at: new Date() });
  }
}