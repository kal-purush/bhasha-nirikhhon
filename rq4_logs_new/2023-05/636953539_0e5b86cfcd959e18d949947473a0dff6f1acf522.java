/**
 * Comprador utiliza el expendedor, entregando una moneda, eligiendo producto, quizas monedas como vuelto e indica que consumio
 */
public class Comprador{
    private String sonido=null;
    private int vuelto=0;

    /**
     *
     * @param m Es la moneda que se recibe como medio de pago
     * @param cualProducto Es el producto que se intentara comprar al expendedor
     * @param exp Es el expendedor elegido para comprar productos
     * @throws PagoIncorrectoException Si la moneda es null, entrega el mensaje de error
     * @throws NoHayProductoException Si no quedan productos del tipo que se pide en el almacen correspondiente, entrega este mensaje de error
     * @throws PagoInsuficienteException Si la moneda es de valor inferior al precio del producto, se entrega este mensaje de error
     */
    public Comprador (Moneda m, int cualProducto, Expendedor exp) throws PagoIncorrectoException,NoHayProductoException,PagoInsuficienteException{
        Producto p = null;
        Moneda m1;
        if ((p = exp.comprarProducto(m,cualProducto))!=null){
            sonido = p.consumir();
        }
        while ((m1 = exp.getVuelto())!=null){
            vuelto = vuelto + m1.getValor();
        }

    }

    /**
     * @return retorna la cantidad total del vuelto que se recibe, si es que es el caso
     */
    public int cuantoVuelto(){
        return vuelto;
    }

    /**
     * @return Retorna un string indicando el nombre del producto que consumio
     */
    public String queConsumio(){
        return sonido;
    }
}