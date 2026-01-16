import { Get, Post, Body, Patch, Param, Delete, Injectable } from '@nestjs/common';
import { CrudService } from './crud.service';
import ICrudController from './interfaces/crud.controller';

@Injectable()
export class CrudController<T, CreateDTO, UpdateDTO> implements ICrudController<T, CreateDTO, UpdateDTO> {
  
  constructor(
    private readonly service: CrudService<T, CreateDTO, UpdateDTO>
  ) {}

  @Post()
  create(@Body() body: CreateDTO): Promise<void> {
    return this.service.create(body);
  }

  @Get()
  async findAll(): Promise<T[]> {
    return this.service.findAll();
  }

  @Get(':id')
  async findById(@Param('id') id: string): Promise<T> {
    return this.service.findById(id);
  }

  @Patch(':id')
  async update(@Param('id') id: string, @Body() update: UpdateDTO): Promise<void> {
     this.service.update(id, update);
  }

  @Delete(':id')
  async delete(@Param('id') id: string): Promise<void> {
    this.service.delete(id);
  }
  
}