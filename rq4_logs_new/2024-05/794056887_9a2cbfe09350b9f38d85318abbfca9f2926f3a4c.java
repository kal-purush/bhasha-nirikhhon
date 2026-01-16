package net.hiperdino.productos;

public class Alimento  extends Producto{

  private boolean caducado;

  private enum Tipo  {
    LACTEO,
    CONGELADO,
    FRESCO,
    PREPARADO,
    PROCESADO
  }
  private Tipo tipo;
  
  public Alimento(String nombre, float precio, boolean caducado, Tipo tipo) {
    super(nombre, precio);
    this.caducado = caducado;
    this.tipo = tipo;
  }

  public boolean isCaducado() {
    return caducado;
  }

  public void setCaducado(boolean caducado) {
    this.caducado = caducado;
  }

  public Tipo getTipo() {
    return tipo;
  }

  public void setTipo(Tipo tipo) {
    this.tipo = tipo;
  }
  
}