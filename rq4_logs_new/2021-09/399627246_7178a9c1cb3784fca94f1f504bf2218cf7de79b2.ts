import { AbstractController } from "./abstract.controller";
import { HorarioPromocaoProduto } from '../../domain/entities/horario_promocao_produto.entity';
import { HorarioPromocaoProdutoRepository } from "../../domain/repositories/horario_promocao_produto.repository";
import { HorarioPromocaoProdutoDTO } from "../../domain/dto/horario_promocao_produto.dto";
import { ReturnMessage } from "../../infraestructure/return_message";
import { HttpRequestCode } from "../../infraestructure/enum/http_request_code.enum";
import { HorarioPromocaoProdutoService } from "../services/horario_promocao_produto.service";

export class HorarioPromocaoProdutoController extends AbstractController<HorarioPromocaoProduto> {

  constructor() {
    super(new HorarioPromocaoProdutoService(new HorarioPromocaoProdutoRepository()));
  }
  
  async get(id: number) {
    try {
      return await new ReturnMessage(await this.service.find(id));
    } catch (error) {
      return new ReturnMessage(Error(error).message).setStatusCode(HttpRequestCode.BadRequest);
    }
  }
  
  async add(entidade: HorarioPromocaoProdutoDTO) {
    try {
      return await new ReturnMessage('');
    } catch (error) {
      return new ReturnMessage(Error(error).message).setStatusCode(HttpRequestCode.BadRequest);
    }
  }
  
}