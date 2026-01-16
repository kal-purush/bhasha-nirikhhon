import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { CustomLogger } from 'src/utils/custom-logger';
import { EntityCondition } from 'src/utils/types/entity-condition.type';
import { Nullable } from 'src/utils/types/nullable.type';
import { DeepPartial, Repository } from 'typeorm';
import { OrdemPagamentoEntity } from '../entity/ordens-pagamento.entity';
import { OrdemPagamentoAgrupadoEntity } from '../entity/ordens-pagamentos-agrupadas.entity';

@Injectable()
export class OrdemPagamentoAgrupadoRepository {
  private logger = new CustomLogger(OrdemPagamentoAgrupadoRepository.name, { timestamp: true });

  constructor(
    @InjectRepository(OrdemPagamentoAgrupadoRepository)
    private ordemPagamentoAgrupadoRepository: Repository<OrdemPagamentoAgrupadoEntity>,
  ) {}

  public async save(dto: DeepPartial<OrdemPagamentoEntity>): Promise<OrdemPagamentoAgrupadoEntity> {
    const createdOrdem = this.ordemPagamentoAgrupadoRepository.create(dto);
    return this.ordemPagamentoAgrupadoRepository.save(createdOrdem);
  }

  public async findOne(fields: EntityCondition<OrdemPagamentoEntity>): Promise<Nullable<OrdemPagamentoAgrupadoEntity>> {
    return await this.ordemPagamentoAgrupadoRepository.findOne({
      where: fields,
    });
  }

  public async findAll(): Promise<OrdemPagamentoAgrupadoEntity[]> {
    return await this.ordemPagamentoAgrupadoRepository.find({});
  }
 
}