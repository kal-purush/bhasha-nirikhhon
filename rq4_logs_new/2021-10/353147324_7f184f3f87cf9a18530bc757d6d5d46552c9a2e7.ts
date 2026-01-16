import BusinessError, { ErrorCodes } from  '@core/errors/business';
import { SituaçãoSolicitacao, TipoSolicitacao } from '@core/models';
import { inject, injectable } from 'inversify';
import { DateTime } from 'luxon';
import EntidadeSolicitacao from '@core/entities/solicitacao';
import { IRepositorySolicitacao } from '@core/repositories/interfaces/solicitacao';
import { IServiceSolicitacao } from '@app/services/interfaces/solicitacao';
import TYPES from '@core/types';

@injectable()
export class ServiceSolicitacao implements IServiceSolicitacao {
  private repositorySolicitacao: IRepositorySolicitacao;

  constructor(
  @inject(TYPES.RepositorySolicitacao) repositorySolicitacao: IRepositorySolicitacao,
  ) {
    this.repositorySolicitacao = repositorySolicitacao;
  }

  async criarSolicitacao(solicitacao: EntidadeSolicitacao, idSolicitante: string): Promise<EntidadeSolicitacao> {
    if (solicitacao.tipo !== TipoSolicitacao.ESPECIALIDADE && solicitacao.tipo !== TipoSolicitacao.GENERO_MUSICAL) {
      throw new BusinessError(ErrorCodes.ARGUMENTOS_INVALIDOS);
    }

    return this.repositorySolicitacao.create({
      nome: solicitacao.nome,
      tipo: solicitacao.tipo,
      situacao: SituaçãoSolicitacao.PENDENTE,
      idSolicitante,
      dataInclusao: DateTime.local().toFormat('yyyy-LL-dd'),
    });
  }
}