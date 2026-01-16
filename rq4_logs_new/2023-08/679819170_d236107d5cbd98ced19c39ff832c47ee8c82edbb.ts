import {
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  Param,
  Post,
  Put,
} from '@nestjs/common';
import { DepoimentosService } from './depoimentos.service';
import { CreateDepoimentoDto } from './Dto/create.depoimento.dto';
import { UpdateDepoimentoDto } from './Dto/update.depoimento.dto';

@Controller('depoimentos')
export class DepoimentosController {
  constructor(private readonly service: DepoimentosService) {}

  @Get()
  async findAll() {
    return await this.service.findAll();
  }

  @Get(':id')
  async findById(@Param('id') id: string) {
    return await this.service.findById(id);
  }

  @Post()
  @HttpCode(201)
  async create(@Body() body: CreateDepoimentoDto) {
    await this.service.create(body);
    return {
      mensagem: 'Depoimento criado com sucesso',
    };
  }

  @Put(':id')
  async update(@Param('id') id: string, @Body() body: UpdateDepoimentoDto) {
    await this.service.update({ id, body });
    return {
      mensagem: 'Depoimento atualizado com sucesso',
    };
  }

  @Delete(':id')
  async delete(@Param('id') id: string) {
    await this.service.delete(id);
    return {
      mensagem: 'Depoimento deletado com sucesso',
    };
  }
}