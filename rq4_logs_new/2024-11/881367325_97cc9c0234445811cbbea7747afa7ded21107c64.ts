import { Filters } from "../../domain";
import { ReportesRepository } from "../../infrastructure/repositories/reportes.repository";


export class ReporteProductosController {

    private repository: ReportesRepository;

    constructor() {
        this.repository = new ReportesRepository()
    }

    async generarReporte(filters: Filters) {
        const totalProductos = await this.repository.totalProductos(filters)
        const total = totalProductos[0].total
        const paginas = Math.round(total / filters.limit)
        const resultado = await this.repository.obtenerReporteProductos(filters)
        return { pagination: { total, paginas, items_por_pagina: filters.limit, offset: filters.offset }, resultado }
    }


}