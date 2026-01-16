import { getCustomRepository } from "typeorm";

import AppError from "../../../shared/errors/AppError";
import Imeis from "../typeorm/entity";
import { ImeisCustomRepository } from "../typeorm/repository";

class DeleteImeisService {
  public async execute(id: string) {
    const imeisRepository = getCustomRepository(ImeisCustomRepository);
    const imei = await imeisRepository.findById(id);
    if (!imei) throw new AppError("Imei not found");
    imeisRepository.remove(imei);
  }
}

export default new DeleteImeisService();