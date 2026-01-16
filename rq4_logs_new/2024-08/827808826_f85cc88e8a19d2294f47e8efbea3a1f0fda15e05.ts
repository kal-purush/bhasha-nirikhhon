import { Body, Controller, Delete, Get, Param, ParseUUIDPipe, Post, Put } from '@nestjs/common';
import { PeopleService } from './people.service';
import Client from './creators/client.model';

@Controller('client')
export class clientController {
    constructor(private readonly peopleService: PeopleService){}

  // Client Endpoints
    @Post()
    createClient(
    @Body('name') name: string,
    @Body('cpf') cpf: string,
    ): Client {
    return this.peopleService.createClient(name,cpf);
   }
  @Get(":id")
  findClientByID(
    @Param('id',new ParseUUIDPipe({ version: '4' })) id: string): Client {
    return this.peopleService.findClientById(id)
  }
  @Put(':id')
  updateClient(
    @Param('id',new ParseUUIDPipe({ version: '4' })) id: string,
    @Body("name") name: string,
    @Body("cpf") cpf: string,
  ): Client{
    return this.peopleService.updateClient(id,name,cpf)
  }
  @Delete(":id")
  deactiveClient(
    @Param("id",new ParseUUIDPipe({ version: '4' })) id: string
  ):Client{
    return this.peopleService.deactivateClientById(id)
  }


}