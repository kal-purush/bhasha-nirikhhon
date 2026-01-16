import { Request } from "express";
import { getCustomRepository } from "typeorm";

import Imeis from "../typeorm/entity";
import { ImeisCustomRepository } from "../typeorm/repository";

class CreateImeisService {
  public async execute(req: Request): Promise<Imeis | undefined> {
    const imeisRepository = getCustomRepository(ImeisCustomRepository);
    let isImei = imeisRepository.findByImei(req.params.imei1);
    if (!isImei) {
      if (req.params?.imei2) {
        isImei = imeisRepository.findByImei(req.params.imei2);
      }
    }
    return isImei;
  }
}

export default new CreateImeisService();