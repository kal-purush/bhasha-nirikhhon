/**
    * @NApiVersion 2.0
    * @NScriptType Restlet
    * @Autor Desarrollador Juan Lancheros
    * @Name AXA | EXTRAER AXA | DETALLES BANCARIOS 
*/

define(['N/log', 'N/search'],
    // Id sandbox: 
    // Id Produccion: 2377

    function (log, search) {
        function _get(context) {
            var strStar =  context.start;
            var strend =  context.end;

            // Cargar la búsqueda AXA | Articulos Activos
            var busqueda_Item_Onhand = search.load({
                // id: '', //Sandbox
                id: 'customsearch_axa_detalles_bancarios_prov', //Produccion
            });


            // Correr la búsqueda
            var resultado_search = busqueda_Item_Onhand.run().getRange({ start: parseInt(strStar), end: parseInt(strend) });
            var cadena = resultado_search


            // Retornar respuesta
            return cadena;
        }

        return {
            get: _get
        }
    }
);