import java.util.LinkedList;
import java.util.Queue;

class BuzonReproceso {
    private Queue<Producto> productos;

    public BuzonReproceso() {
        this.productos = new LinkedList<>();
    }

    public synchronized void depositar(Producto producto) throws InterruptedException {
        System.out.println("se depositó en buzon de reproceso " + producto.getNombre());
        productos.add(producto);
        notifyAll(); // Notifica a los hilos en espera
    }

    public synchronized Producto retirar() throws InterruptedException {
        while (productos.isEmpty()) {
            try{

            
            wait(); }
            catch(InterruptedException e){
                Thread.currentThread().interrupt();
            }
            // Espera pasiva si está vacío
        }
        return productos.poll();
    }

    public synchronized boolean estaVacio() {
        return productos.isEmpty();
    }
}