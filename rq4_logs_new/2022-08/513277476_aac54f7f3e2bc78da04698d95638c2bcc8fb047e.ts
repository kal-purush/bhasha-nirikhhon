import { Request, Response } from "express"
import { container } from "tsyringe";
import { CriarAssuntoUseCase } from "./CriarAssuntoUseCase";

class CriarAssuntoController {
  async handle(request: Request, response: Response) {
    const { nome } = request.body;

    const criarAssuntoUseCase = container.resolve(CriarAssuntoUseCase);

    const criar = await criarAssuntoUseCase.execute({ nome });

    if (criar) {
      return response.status(201).send();
    }
    else {
      return response.status(400).json({ error: "Assunto já cadastrado" });
    }

  }
}

export { CriarAssuntoController };