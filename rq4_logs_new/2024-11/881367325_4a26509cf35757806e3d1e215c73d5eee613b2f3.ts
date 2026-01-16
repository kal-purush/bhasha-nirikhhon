

export const productosDoc = {

    get: {
        tags: ['Reportes'],
        description: "Reporte sobre productos",
        responses:{
            default:{
                description:"Respuesta por defecto"
            }
        }
    },
    post: {
        tags: ['Reportes'],
        description: "Reporte sobre productos",
        requestBody: {
            required: true,
            description: "Body del reporte de productos",
            content: {
                "application/json": {
                    schema: {
                        "$ref": "#/components/schemas/Reporte"
                    },
                    example: {
                        limit: 10,
                        search: "lima"
                    }
                }
            }
        }
    }
}