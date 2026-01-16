import { Repository, getRepository } from 'typeorm';
import EntidadeSolicitacao from '@core/entities/solicitacao';
import { IRepositorySolicitacao } from './interfaces/solicitacao';
import { injectable } from 'inversify';

@injectable()
export class RepositorySolicitacao implements IRepositorySolicitacao {
  private repositorySolicitacao: Repository<EntidadeSolicitacao> = getRepository(EntidadeSolicitacao);

  async create(solicitacao: EntidadeSolicitacao): Promise<EntidadeSolicitacao> {
    return this.repositorySolicitacao.save(solicitacao);
  }

  async selectById(id: string): Promise<EntidadeSolicitacao> {
    return this.repositorySolicitacao.findOne({ where: { id } });
  }
}