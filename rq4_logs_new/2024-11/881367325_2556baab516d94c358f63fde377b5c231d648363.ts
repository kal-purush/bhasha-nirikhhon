import { query } from "express"
import { getPoolConnection } from "../../src/infrastructure/repositories/data-source"
import { ReportesRepository } from "../../src/infrastructure/repositories/reportes.repository"

// NOTA: Importante que la ruta sea la correcta al módulo o función que se quiera simular
jest.mock('../../src/infrastructure/repositories/data-source', () => ({
    getPoolConnection: jest.fn()
}))

describe('ReportesRepository', () => {

   
    let mockQuery = jest.fn()

    beforeAll(() => {

    })

    beforeEach(() => {
        mockQuery.mockClear()
    })

    test('crear una instancia de tipo ReportesRepository ', () => {
        const instance = new ReportesRepository()
        expect(instance).toBeInstanceOf(ReportesRepository)
    })

    test('obtener el totalProductos correctamente', async () => {
        const valorSimulado = [{ total: 10 }]
        // Simula la linea 13: query<RowDataPacket[]>(querySql);
        mockQuery = jest.fn().mockResolvedValue(valorSimulado);
        // Simula la linea 13: connection.query 
        // Donde le digo al getPoolConnection que tiene una función query
        (getPoolConnection as jest.Mock).mockReturnValue({ query: mockQuery })
        const repository = new ReportesRepository()
        const total = await repository.totalProductos({ limit: 100, offset: 0 })
        expect(total).toEqual({ total: 10 })
    })

    /**
     * Objetivo de la prueba:
     * Si yo invoco el método obtenerReporteProductos(filtro), debo de recibir un 
     * array de productos por ejemplo:
     * [ {id:'', nombre:'', ...el resto de propiedades },  {}, {}]
     * 
     * Si prueba falla si no retorna un array
     */
    test('método obtenerReporteProductos debe retornar un array con productos', async () => {
        const valorSimulado = [
            [
                {
                    "id": 100,
                    "nombre": "Mango",
                    "descripcion": "Mango",
                    "precio": "10000",
                    "cantidad_disponible": 100
                },
                {
                    "id": 18,
                    "nombre": "Arroz",
                    "descripcion": "Arroz blanco colombiano",
                    "precio": "3200",
                    "cantidad_disponible": 50
                }
            ],
            [{
                columa: 1,
                nombre: 'id'
            }]
        ];
        // // Simula el método query de la linea 21:  query<RowDataPacket[]>(querySql)
        mockQuery = jest.fn().mockResolvedValue(valorSimulado);
        (getPoolConnection as jest.Mock).mockReturnValue({
            query: mockQuery
        });

        const repository = new ReportesRepository()
        const reporte = await repository.obtenerReporteProductos({ limit: 100, offset: 0 })

        expect(reporte).toEqual(valorSimulado[0])
    })





})

