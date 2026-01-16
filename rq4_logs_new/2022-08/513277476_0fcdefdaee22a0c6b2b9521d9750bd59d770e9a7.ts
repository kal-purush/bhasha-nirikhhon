import { Request, Response } from "express"
import { container } from "tsyringe";
import { AddAssuntoConteudoUseCase } from "./AddAssuntoConteudoUseCase";

class AddAssuntoConteudoController {
  async handle(request: Request, response: Response): Promise<Response> {
    const { assunto, conteudo } = request.body;
    const addAssuntoConteudoUseCase = container.resolve(AddAssuntoConteudoUseCase);

    try {
      await addAssuntoConteudoUseCase.execute({
        assunto,
        conteudo,
      });

      return response.status(201).send();
    } catch (err) {
      return response.status(400).json({
        message: err.message || 'Unexpected error.',
      });
    }
  }
}

export { AddAssuntoConteudoController };