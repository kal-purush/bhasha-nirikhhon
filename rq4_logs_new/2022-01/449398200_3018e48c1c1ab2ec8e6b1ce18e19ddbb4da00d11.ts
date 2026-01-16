import { getCustomRepository } from "typeorm";

import AppError from "../../../shared/errors/AppError";
import removeFile, { verify } from "../../../shared/functions/removeFiles";
import Notebooks from "../typeorm/entity";
import NotebooksCustomRepository from "../typeorm/repository";

interface INotebook {
  id: string;
  marca?: string;
  modelo?: string;
  imgPath?: string;
  imgName?: string;
  numeroSerie?: string;
  numeroPatrimonio?: string;
}

class UpdateNotebookService {
  public async execute(data: INotebook): Promise<Notebooks> {
    const notebooksRepository = getCustomRepository(NotebooksCustomRepository);
    const notebookOld = await notebooksRepository.findOne({
      where: { id: data.id },
    });

    if (!notebookOld) throw new AppError("notebook not found");

    if (await verify(notebookOld.imgPath))
      await removeFile(notebookOld.imgPath);

    const notebookUpdate = await notebooksRepository.merge(notebookOld, data);
    await notebooksRepository.save(notebookUpdate);
    return notebookUpdate;
  }
}

export default new UpdateNotebookService();