import java.util.List;
import java.util.ArrayList;

public class Venta
{
    private List<Producto> listaCarrito;
    private Almacen almacen;
    
    public Venta()
    {
        almacen = new Almacen();
        listaCarrito = new ArrayList<Producto>();
    }
    
    public List<Producto> getListaCarrito()
    {
        return this.listaCarrito;
    }
    
    public boolean agregarACarrito(Producto p, int cant)
    {
        if (cant > p.getCantidad())
        {
            return false;
        }
        else 
        {
            p.setCantidad(cant);
            listaCarrito.add(p);    
            return true;
        }
    }
    
    public int calcularVenta()
    {
        int totalVenta = 0;
        for (Producto p: listaCarrito)
        {
            totalVenta = totalVenta + (p.getCantidad() * p.getPrecio());
        }
        return totalVenta;
    }
    
    public void finalizarVenta()
    {
        for (Producto p: listaCarrito)
        {
            almacen.disminuirCantProducto(p.getCodigo(), p.getCantidad());
        }
        almacen.actualizarArchivo();
    }
}