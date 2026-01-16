import { getCustomRepository } from "typeorm";

import AppError from "../../../shared/errors/AppError";
import Notebooks from "../typeorm/entity";
import NotebooksCustomRepository from "../typeorm/repository";

interface IRequest {
  imgName?: string;
  imgPath?: string;
  marca: string;
  modelo: string;
  numeroPatrimonio?: string;
  numeroSerie?: string;
}

class CreateNotebookService {
  public async Execute(data: IRequest): Promise<Notebooks> {
    const notebookRepository = getCustomRepository(NotebooksCustomRepository);

    if (data.numeroPatrimonio) {
      const numeroPatrimonio = await notebookRepository.findByNumeroPatrimonio(
        data.numeroPatrimonio
      );
      if (numeroPatrimonio)
        throw new AppError("Numero de patrimonio already exists");
    }

    if (data.numeroSerie) {
      const numeroSerie = await notebookRepository.findByNumeroSerio(
        data.numeroSerie
      );
      if (numeroSerie) throw new AppError("Numero de serie already exists");
    }

    const notebook = await notebookRepository.create(data);
    await notebookRepository.save(notebook);

    return notebook;
  }
}

export default new CreateNotebookService();