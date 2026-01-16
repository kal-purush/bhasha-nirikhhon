package tarea;

public class Inicializar {
    private Moneda mon = null;
    private int eleccionProducto = 0;
    private Expendedor exp = null;
    private Comprador com = null;

    public Inicializar(){
        exp= new Expendedor(4, 500, 300);
    }

    public void setMoneda(Moneda moneda){
        mon = moneda;
    }
    public Moneda getMoneda(){
        return mon;
    }
    public void setEleccionProducto(int n){
        eleccionProducto = n;
    }
    public int getEleccion(){
        return eleccionProducto;
    }
    public Expendedor getExp(){
        return exp;
    }

    public Comprador setCom() throws NoHayProductoException, PagoInsuficienteException, PagoIncorrectoException {
        com = new Comprador(getMoneda(), eleccionProducto, getExp());
        return com;
    }
}