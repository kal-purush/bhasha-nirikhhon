import { getCustomRepository, Like } from "typeorm";

import Colaboradores from "../typeorm/entity";
import ColaboradoresRepository from "../typeorm/repository";

class FindForNameOrIdentityColaboradorService {
  public async execute(data: string): Promise<Colaboradores[] | undefined> {
    const colaboradorRepository = getCustomRepository(ColaboradoresRepository);
    const colaborador = await colaboradorRepository.find({
      where: [{ id: data }, { nome: Like(`%${data}%`) }],
    });

    return colaborador;
  }
}

export default new FindForNameOrIdentityColaboradorService();