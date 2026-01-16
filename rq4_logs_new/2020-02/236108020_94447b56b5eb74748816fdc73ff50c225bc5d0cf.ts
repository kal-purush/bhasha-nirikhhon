import { BadRequestException, Body, Controller, Delete, Get, Param, Post, Req, Session } from '@nestjs/common';
import { ProveedorService } from './proveedor.service';
import { ProveedorEntity } from './proveedor.entity';
import { ProveedorCreateDto } from './proveedor.create-dto';
import { validate } from 'class-validator';
import { DeleteResult } from 'typeorm';
import { ProveedorLogingDto } from './proveedor.loging-dto';

@Controller('proveedor')
export class ProveedorController {
  // tslint:disable-next-line:variable-name
  constructor(
    private readonly _proveedorService: ProveedorService,
  ) {
  }

  @Get('sayhey')
  getHello() {
    return 'proveedor';
  }

  @Post()
  async crearUsuario(
    @Body() cliente: ProveedorEntity,
  ): Promise<ProveedorEntity> {
    let clienteDTO = new ProveedorCreateDto();
    clienteDTO.nombre = cliente.nombre;
    clienteDTO.apellido = cliente.apellido;
    clienteDTO.cedula = cliente.numeroCedula;
    clienteDTO.contrasena = cliente.contrasena;
    clienteDTO.correo = cliente.correo;
    const validacion=await validate(clienteDTO);
    console.log(validacion);
    if (validacion.length === 0) {
      try {
        return this._proveedorService.crearUno(cliente);
      }catch (e) {
        console.log(e);
      }
    } else {
      throw new BadRequestException('Error en validacion');
    }
  }

  @Delete(':id')
  eliminarCliente(
    @Param('id') idcliente: string,
  ): Promise<DeleteResult> {
    try {
      return this._proveedorService.borrarUno(+idcliente);
    } catch (e) {
      console.log(e);
    }
  }

}
