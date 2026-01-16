import {Body, Controller, Get, Param, Post, Put, Req, Res} from "@nestjs/common";
import {ComidaService} from "./comida.service";
import {COMIDA_SCHEMA} from "./comida.schema";
import {GeneralPipe} from "../general.pipe";
import {ComidaClass} from "./comida.class";
import {NoEncontradoException} from "../exceptions/noEncontrado.exception";

@Controller('Comida')
export class ComidaController {

    constructor(private _comidaService: ComidaService) {}

    @Get()
    listarTodos(
        @Res() response,
        @Req() request,
    ) {
        const comidas = this._comidaService.mostrarTodos();
        return response.send(comidas);
    }

    @Post()
    crearComida(
        @Body(new GeneralPipe(COMIDA_SCHEMA)) comidaArgumento
    ): ComidaClass[] {
        const comida = comidaArgumento;
        return this._comidaService.agregarComida(comida);
    }

    @Get('/:id')
    obtenerUno(
        @Param() paramParams,
        @Res() response
    ) {
        const comidaPorId = this._comidaService.mostrarUnoPorId(paramParams.id);

        if (comidaPorId === undefined) {
            throw new NoEncontradoException(
                'No se encontro ningun elemento con ese id',
                'error',
                4
            )
        } else {
            return response.send(comidaPorId);
        }
    }

    @Put('/:id')
    editarUno(
        @Param() paramParams,
        @Body(new GeneralPipe(COMIDA_SCHEMA)) comidaArgumento,
        @Res() response
    ) {
        if (this._comidaService.mostrarUnoPorId(paramParams.id) !== undefined) {
            return response.send(this._comidaService.editarUnoPorId(paramParams.id, comidaArgumento));
        } else {
            throw new NoEncontradoException(
                'No se encontro ningun elemento para editar con ese id',
                'error',
                4
            )
        }
    }
}