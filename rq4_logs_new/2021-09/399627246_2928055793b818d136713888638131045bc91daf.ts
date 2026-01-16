import { AbstractController } from "./abstract.controller";
import { PromocaoProduto } from '../../domain/entities/promocao_produto.entity';
import { PromocaoProdutoService } from '../services/promocao_produto.service';
import { PromocaoProdutoRepository } from "../../domain/repositories/promocao_produto.repository";

export class PromocaoProdutoController extends AbstractController<PromocaoProduto> {

  constructor() {
    super(new PromocaoProdutoService(new PromocaoProdutoRepository()));
  }
  
}