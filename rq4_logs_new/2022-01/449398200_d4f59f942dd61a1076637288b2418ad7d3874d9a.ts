import { getCustomRepository } from "typeorm";

import AppError from "../../../shared/errors/AppError";
import NotebooksCustomRepository from "../typeorm/repository";

class DeleteUpdateService {
  public async execute(id: string) {
    const notebookRepository = getCustomRepository(NotebooksCustomRepository);
    const notebook = await notebookRepository.findOne({ where: { id } });

    if (!notebook) throw new AppError("Notebook not found");

    await notebookRepository.remove(notebook);
  }
}

export default new DeleteUpdateService();