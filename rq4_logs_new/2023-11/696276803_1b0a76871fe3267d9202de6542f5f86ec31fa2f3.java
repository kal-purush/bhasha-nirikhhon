public class Car {

    public Car(int numeroDiPorte, String colore, String propulsione, String tipo, int serbatoio) {
        this.numeroDiPorte = numeroDiPorte;
        this.colore = colore;
        this.propulsione = propulsione;
        this.tipo = tipo;
        this.serbatoio = serbatoio;
    }

    private int numeroDiPorte;
    private String colore;
    private String propulsione;
    private String tipo;
    private int serbatoio;

    @Override
    public String toString() {
        return "Car{" +
                "numeroDiPorte=" + numeroDiPorte +
                ", colore='" + colore + '\'' +
                ", propulsione='" + propulsione + '\'' +
                ", tipo='" + tipo + '\'' +
                ", serbatoio=" + serbatoio +
                '}';
    }
}