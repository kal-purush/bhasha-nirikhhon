import { EntityRepository, Repository } from "typeorm";

import ColaboradoresEntity from "../entity/index";

@EntityRepository(ColaboradoresEntity)
class ColaboradoresRepository extends Repository<ColaboradoresEntity> {
  public async findByIdentificacao(
    identificacao: string
  ): Promise<ColaboradoresEntity | undefined> {
    const colaborador = await this.findOne({ where: { identificacao } });
    return colaborador;
  }

  public async findByName(
    nome: string
  ): Promise<ColaboradoresEntity | undefined> {
    const colaborador = await this.findOne({ where: { nome } });
    return colaborador;
  }
}

export default ColaboradoresRepository;