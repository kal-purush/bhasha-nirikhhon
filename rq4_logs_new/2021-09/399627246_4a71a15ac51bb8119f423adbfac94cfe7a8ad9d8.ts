import { AbstractController } from "./abstract.controller";
import { HorarioFuncionamento } from '../../domain/entities/horario_funcionamento.entity';
import { HorarioFuncionamentoService } from '../services/horario_funcionamento.service';
import { HorarioFuncionamentoRepository } from '../../domain/repositories/horario_funcionamento.repository';

export class HorarioFuncionamentoController extends AbstractController<HorarioFuncionamento> {

  constructor() {
    super(new HorarioFuncionamentoService(new HorarioFuncionamentoRepository()));
  }
  
}