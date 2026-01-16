import { container } from 'tsyringe';
import { Request, Response } from 'express';
import SelecionarInscricaoAlunoIcService from '@modules/vagas_ic/services/SelecionarInscricaoAlunoService';

export default class InscricoesSelecionadasController {
  public async update(request: Request, response: Response): Promise<Response> {
    const { id } = request.params;

    const selecionarInscricaoAluno = container.resolve(
      SelecionarInscricaoAlunoIcService,
    );

    await selecionarInscricaoAluno.execute(id);

    return response.json({ message: 'Aluno selecionado para a vaga de IC.' });
  }
}