import {Body, Controller, Get, Param, Post, Res} from "@nestjs/common";
import {IngredienteService} from "../ingrediente/ingrediente.service";
import {TransferenciaService} from "./transferencia.service";
import {TransferenciaEntity} from "./transferencia.entity";

@Controller('transferencia')
export class TransferenciaController {

    constructor(private _transferenciaService: TransferenciaService) {}

    @Get('peticionesRealizadas/:idUsuario')
    async obtenerPeticionesRealizadasPorUsuario(
        @Param() paramParams,
        @Res() response
    ) {
        const usuarios = await this._transferenciaService.traerPeticionesRealizadasPorUsuario(paramParams.idUsuario);
        return response.send(usuarios);
    }
    @Get('peticionesRecibidas/:idUsuario')
    async obtenerPeticionesRecibidasPorUsuario(
        @Param() paramParams,
        @Res() response
    ) {
        const usuarios = await this._transferenciaService.traerPeticionesRecibidasPorUsuario(paramParams.idUsuario);
        return response.send(usuarios);
    }

    @Post('agregar')
    async crear(
        @Body('usuarioPide')idUsuarioPide,
        @Body('usuarioOfrece')idUsuarioOfrece,
        @Body('ingredientePedido')idIngredientePedido,
        @Body('ingredienteOfertado')idIngredienteOfertado,){
        return this._transferenciaService.crearTransferencia(idUsuarioPide, idUsuarioOfrece, idIngredientePedido, idIngredienteOfertado);
    }

    /*@Post('/:usuarioPideId/:usuarioOfreceId/:ingredientePedidoId/:ingredienteOfertadoId')
    async crearTransferenciaBase(
        @Param() paramParams,
        @Res() response
    ) {
        const transferencia = this._transferenciaService.crearTransferencia(paramParams.usuarioPideId, paramParams.usuarioOfreceId, paramParams.ingredientePedidoId, paramParams.ingredienteOfertadoId);
        return response.send(transferencia);
    }*/
    @Post('crearLista')
    async crearTransferenciasBase() {
        const transferencias = this._transferenciaService.crearTransferenciasLista();
        return transferencias;
    }
}