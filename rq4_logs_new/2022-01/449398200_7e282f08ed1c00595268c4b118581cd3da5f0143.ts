import { getCustomRepository } from "typeorm";

import AppError from "../../../shared/errors/AppError";
import ColaboradoresRepository from "../typeorm/repository";

class DeleteColaboradorService {
  public async execute(id: string) {
    const colaboradorRepository = getCustomRepository(ColaboradoresRepository);
    const colaborador = await colaboradorRepository.findById(id);
    if (!colaborador) throw new AppError("Colaborador note found");
    await colaboradorRepository.remove(colaborador);
  }
}

export default new DeleteColaboradorService();