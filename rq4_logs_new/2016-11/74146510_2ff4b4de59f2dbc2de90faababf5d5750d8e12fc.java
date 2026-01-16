package es.adrigala.model;

/**
 * Created by juanxxiii on 18/11/2016.
 */
public class Caballo {
    private int posicion;
    private int numeroCaballo;

    public Caballo(int numeroCaballo) {
        this.numeroCaballo = numeroCaballo;
    }

    public int getPosicion() {
        return posicion;
    }

    public void setPosicion(int posicion) {
        this.posicion = posicion;
    }

    public int getNumeroCaballo() {
        return numeroCaballo;
    }

}