import { v4 as uuidv4 } from 'uuid';

import { MedicFactory } from '../domain/factories/medic.factory';
import { MedicInputs } from '../domain/inputs/input-medic.type';
import { MedicRepository } from '../domain/repositories/medic.repository';

export class MedicApplication {
  /* repository: MedicRepository

    constructor(repository: MedicRepository){
        this.repository = repository;
    } */
  constructor(private readonly repository: MedicRepository) {}

  async createMedic(
    name: string,
    lastname: string,
    cmp: string,
    phone: string,
    email: string
  ): Promise<void> {
    const medicId = uuidv4();
    const inputs: MedicInputs = {
      medicId,
      name,
      lastname,
      cmp,
      phone,
      email,
    };

    const medic = MedicFactory.create(inputs);

    await this.repository.save(medic);
  }
}