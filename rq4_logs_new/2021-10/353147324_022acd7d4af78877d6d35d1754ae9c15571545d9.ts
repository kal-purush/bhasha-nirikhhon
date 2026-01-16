import {
  BaseHttpController,
  controller,
  httpPost,
  interfaces,
} from 'inversify-express-utils';
import EntidadeSolicitacao from '@core/entities/solicitacao';
import { IServiceSolicitacao } from '@app/services/interfaces/solicitacao';
import { Request } from 'express';
import TYPES from '@core/types';
import { TipoSolicitacao } from '@core/models';
import autenticado from '@app/middlewares/autenticado';
import { inject } from 'inversify';

@controller('/solicitacao')
export class ControllerSolicitacao extends BaseHttpController implements interfaces.Controller {
  private serviceSolicitacao: IServiceSolicitacao;

  constructor(
  @inject(TYPES.ServiceSolicitacao) serviceSolicitacao: IServiceSolicitacao,
  ) {
    super();

    this.serviceSolicitacao = serviceSolicitacao;
  }

  @httpPost('/', autenticado)
  private async criarAdmin(req: Request): Promise<EntidadeSolicitacao> {
    return this.serviceSolicitacao.criarSolicitacao({
      nome: req.body.nome as string,
      tipo: req.body.tipo as TipoSolicitacao,
    }, req.session.userID);
  }
}