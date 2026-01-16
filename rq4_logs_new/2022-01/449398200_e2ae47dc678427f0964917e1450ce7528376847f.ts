import { getCustomRepository } from "typeorm";

import Colaboradores from "../typeorm/entity";
import ColaboradoresRepository from "../typeorm/repository";

interface IRequest {
  identificacao: string;
  nome: string;
  cargo?: string;
}

class CreateColaboradoresService {
  async execute({
    identificacao,
    nome,
    cargo,
  }: IRequest): Promise<Colaboradores> {
    const ColaboradorRepository = getCustomRepository(ColaboradoresRepository);
    const colaborador = await ColaboradorRepository.create({
      identificacao,
      nome,
      cargo,
    });
    return colaborador;
  }
}

export default new CreateColaboradoresService();