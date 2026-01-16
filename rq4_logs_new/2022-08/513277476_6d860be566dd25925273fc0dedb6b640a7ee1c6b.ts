import { Repository } from "typeorm";
import { AppDataSource } from "../../../../DockerDataSource";
import { Assunto } from "../../entities/Assunto";
import { IAssuntosRepository, ICreateAssuntoDTO } from "../IAssuntosRepository";

class AssuntosRepository implements IAssuntosRepository {
  private repository: Repository<Assunto>;

  constructor() {

    this.repository = AppDataSource.getRepository(Assunto);

  }
  async create({ nome }: ICreateAssuntoDTO): Promise<void> {
    const assunto = this.repository.create({ nome });
    await this.repository.save(assunto);
  }

  async list(): Promise<Assunto[]> {
    const assuntos = await this.repository.find();
    return assuntos;
  }

  async findByName(nome: string): Promise<Assunto> {
    const assunto = await this.repository.findOne({ where: { nome } });
    return assunto;
  }

  async delete(id: string): Promise<void> {
    await this.repository.delete(id);
  }

  async update(nome: string): Promise<void> {
    const assunto = await this.findByName(nome);
    assunto.nome = nome;
    await this.repository.save(assunto);
  }
}

export { AssuntosRepository };