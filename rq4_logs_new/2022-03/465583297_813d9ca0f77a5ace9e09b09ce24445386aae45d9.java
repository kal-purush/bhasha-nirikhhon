package test;

import controladores.AdministradorProductos;
import controladores.AdministradorVentaCompra;
import modelos.Productos;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AdministradorVentaCompraTest{

    @Test
    void verificarExistenciasBicicletasGamaMedia(){
        AdministradorVentaCompra  venta = new AdministradorVentaCompra();
        AdministradorProductos producto = new AdministradorProductos();
        producto.agregarProductosPorDefecto();
        
System.out.println("valor Final"+venta.validarExistencias("BIC28371","3"));
        //assertEquals(11,venta.validarExistencias("BIC28371","3"));

    }
}