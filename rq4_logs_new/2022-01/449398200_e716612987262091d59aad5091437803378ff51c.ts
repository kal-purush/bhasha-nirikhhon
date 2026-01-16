import { Request } from "express";
import { getCustomRepository } from "typeorm";

import AppError from "../../../shared/errors/AppError";
import Imeis from "../typeorm/entity";
import { ImeisCustomRepository } from "../typeorm/repository";

class CreateImeisByCelularService {
  public async execute(req: Request): Promise<Imeis | undefined> {
    const imeisRepository = getCustomRepository(ImeisCustomRepository);
    let isImei = await imeisRepository.findByImei(req.params.imei1);
    if (!isImei && req.params.imei2) {
      isImei = await imeisRepository.findByImei(req.params.imei2);
    }
    if (isImei) throw new AppError("Imei already exists");

    const imei = await imeisRepository.create({ ...req.params });
    await imeisRepository.save(imei);
    return imei;
  }
}

export default new CreateImeisByCelularService();