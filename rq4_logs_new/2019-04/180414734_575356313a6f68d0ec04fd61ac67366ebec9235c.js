
Ext.define('CDT.store.transporte.especialista.TransporteStore', {
    extend   : 'Ext.data.Store',
    fields   : [ 'mes', 'incidencia' ],
    autoLoad : true,
    
    data: [
        { mes: 'Enero',    incidencia: 16, type: 'int'},
        { mes: 'Febrero' , incidencia: 14, type: 'int'},
        { mes: 'Marzo',    incidencia: 8, type: 'int'},
        { mes: 'Abril',    incidencia: 5, type: 'int'}
    ]
});