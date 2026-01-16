import { Request } from "express";
import { getCustomRepository } from "typeorm";

import AppError from "../../../shared/errors/AppError";
import Celulares from "../typeorm/entity";
import { CelularesCustomRespository } from "../typeorm/repository";

interface ICelular {
  id: string;
  marca?: string;
  molelo?: string;
}

class UpdateCelularService {
  public async execute(req: Request): Promise<Celulares> {
    const newData = {} as ICelular;
    Object.assign(newData, req.body);
    const celularRepository = getCustomRepository(CelularesCustomRespository);
    const celularOld = await celularRepository.findOne({
      where: { id: newData.id },
    });

    if (!celularOld) throw new AppError("Celular not found");
    const celularUpdated = await celularRepository.merge(celularOld, newData);
    await celularRepository.save(celularUpdated);
    return celularUpdated;
  }
}

export default new UpdateCelularService();