import { getCustomRepository } from "typeorm";

import AppError from "../../../shared/errors/AppError";
import { CelularesCustomRespository } from "../typeorm/repository";

class DeleteCelularService {
  public async execute(id: string) {
    const celularRepository = getCustomRepository(CelularesCustomRespository);
    const celular = await celularRepository.findOne({ where: { id } });
    if (!celular) throw new AppError("Celular not found");
    await celularRepository.remove(celular);
  }
}

export default new DeleteCelularService();