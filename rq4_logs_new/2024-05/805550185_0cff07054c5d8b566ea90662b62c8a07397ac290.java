public class Carta {
  private int valor;
  private int palo;
  private boolean visible;

  // Constructor
  public Carta(int valor, int palo, boolean visible) {
    this.valor = valor;
    this.palo = palo;
    this.visible = visible;
  }

  public int getValor() {
    return valor;
  }

  public void setValor(int valor) {
    this.valor = valor;
  }

  public int getPalo() {
    return palo;
  }

  public void setPalo(int palo) {
    this.palo = palo;
  }

  public boolean isVisible() {
    return visible;
  }

  public void setVisible(boolean visible) {
    this.visible = visible;
  }

  // Método toString para representar la carta como una cadena de texto
  @Override
  public String toString() {
    if (visible) {
      String[] palos = { "Corazón", "Diamante", "Trébol", "Espada" };
      String[] valores = { "", "As", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K" };
      return "[" + valores[valor] + " de " + palos[palo - 1] + "]"; // Construir y devolver la representación dela carta
    } else {

      return "[Carta Oculta]";
    }
  }
}