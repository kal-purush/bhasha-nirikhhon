import java.util.LinkedList;
import java.util.Queue;

class Buzon {
    private Queue<String> productos;
    private int capacidad;
    private String tipoBuzon;

    public Buzon(int capacidad, String tipoBuzon) {
        this.productos = new LinkedList<>();
        this.capacidad = capacidad;
        this.tipoBuzon = tipoBuzon;
    }

    public synchronized void depositar(String producto) throws InterruptedException {
        while (productos.size() >= capacidad) {
            wait(); // Espera pasiva si el buzón está lleno
        }
        productos.add(producto);
        notifyAll(); // Notifica a los hilos en espera
    }

    public String retirar() throws InterruptedException {
        while (productos.isEmpty()) {
            if (tipoBuzon.equals("REVISION")) {
                Thread.yield(); // Espera semi-activa
            } else {
                synchronized (this) {
                    wait(); // Espera pasiva para REPROCESO y DEPOSITO
                }
            }
        }

        synchronized (this) {
            String producto = productos.poll();
            notifyAll(); // Notifica a los hilos en espera
            return producto;
        }
    }

    public synchronized boolean estaVacio() {
        return productos.isEmpty();
    }
}