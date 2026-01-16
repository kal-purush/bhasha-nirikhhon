/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package visitor;

/**
 *
 * @author Sandra
 */
public class IVA implements Ivisitante{
    private final double impuestoNormal = 1.21;
    private final double impuestoDescuento = 1.105;
    
    @Override
    public double visit(ProductoNormal normal) {
        return normal.getPrecio() * impuestoNormal;
    }

    @Override
    public double visit(ProductoDescuento descuento) {
        return descuento.getPrecio() * impuestoDescuento;
    }
    
}