/* eslint-disable prettier/prettier */
import { IsNotEmpty, MaxLength } from 'class-validator';
import { CreateProdutoDto } from 'src/produtos/dto/create-produto.dto';

export class CreateProcessadorDto extends CreateProdutoDto{
  /**
    * Informação sobre a quantidade de gigahertz do processador
    * @example 500
    */
  @IsNotEmpty({
    message: 'Informe a quantidade de gigahertz',
  })
  gigahertz: number;

  /**
    * Informação sobre a quantidade de cache do processador
    * @example 100
    */
   @IsNotEmpty({
    message: 'Informe a quantidade de cache',
  })
  cache: number;

}