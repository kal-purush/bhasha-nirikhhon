import { AbstractController } from "./abstract.controller";
import { CategoriaProduto } from '../../domain/entities/categoria_produto.entity';
import { CategoriaProdutoService } from '../services/categoria_produto.service';
import { CategoriaProdutoRepository } from "../../domain/repositories/categoria_produto.repository";

export class CategoriaProdutoController extends AbstractController<CategoriaProduto> {

  constructor() {
    super(new CategoriaProdutoService(new CategoriaProdutoRepository()));
  }
  
}