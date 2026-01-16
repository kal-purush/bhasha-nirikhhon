package modelo;

public class Producto {

    protected int id;
    protected float monto;
    protected Cliente cliente;
    protected Fecha fechaPedio;
    protected Producto producto;
    protected int cantidad;

    public Producto(int id, float monto, int cantidad) {

        setId(id);
        setMonto(monto);
        setCantidad(cantidad);
        cliente = new Cliente(id, nombre, apellidoPaterno, apellidoMaterno, direccion, telefono);
        fechaPedio = new Fecha(dia, mes, anio);
        producto = new Producto(id, nombre, precio, categoria, existencia);

    }

    public void setId(int id){

        this.id = id;

    }

    public void setMonto(float monto){

        this.monto = monto;

    }

    public void setCantidad(int cantidad){

        this.cantidad = cantidad;

    }

    public int getId(){

        return id;

    }

    public float getMonto(){

        return monto;

    }

    public int getCantidad(){

        return cantidad;
        
    }

}