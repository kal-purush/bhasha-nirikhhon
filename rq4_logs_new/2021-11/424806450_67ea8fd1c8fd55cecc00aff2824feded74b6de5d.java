/**
 * Clase Vendedor Clase que simula a un vendedor de autos
 * 
 * @author Melissa Fernández
 * @version 1.0
 */
public class Vendedor {

    private boolean buenHumor; // Si el vendedor tiene buen humor o no
    private boolean trabajador; // Si el vendedor es trabajador o no

    /**
     * Método constructor sin parámetros
     */
    public Vendedor() {
        buenHumor = true;
        trabajador = true;

    }

    /**
     * Método getBuenHumor Método que nos deja saber si el vendedor está de buen
     * humor o no
     * 
     * @return buenHumor Si el vendedor está de buen humor o no
     */
    public boolean getBuenHumor() {
        return buenHumor;
    }

    /**
     * Método setBuenHumor método que permite cambiar el humor del vendedor
     * 
     * @param buenHumor
     */
    public void setBuenHumor(boolean buenHumor) {
        this.buenHumor = buenHumor;
    }

    /**
     * Método presentarse hace que el vendedor se presente
     * 
     * @return La presentación del vendedor
     */
    public String presentarse() {
        return "Hola, buen día, mi nombre es Humberto y estoy aquí para ayudarl@ a elegir su auto. ";
    }

    /**
     * Método preguntarModelo Hace que el vendedor pregunte al usuario el modelo que
     * tiene en mente
     * 
     * @return Pregunta del modelo en mente
     */
    public String preguntarModelo() {
        return "¿Cuál es el modelo que necesita?";
    }

    /**
     * Método ofrecerTipoAuto Hace que el vendedor pregunte el tipo de auto
     * requerido por el cliente
     * 
     * @return La pregunta del tipo de auto requerido
     */
    public String ofrecerTipoAuto() {
        return "¿Qué tipo de auto cree que necesita? (de lujo / deportivo / mediano)";
    }

    /**
     * Método preguntarColor Hace que el vendedor pregunte el color de auto que
     * quiere el cliente
     * 
     * @return La pregunta del color de auto
     */
    public String preguntarColor() {
        return "¿En qué color buscaba su auto?";
    }

    /**
     * Método negarColor hace que el vendedor niegue un color de auto
     * 
     * @return Respuesta que niega un color de auto
     */
    public String negarColor() {
        return "Disculpe, ese color se ha agotado, pero tenemos ese modelo de auto en blanco, negro y azul. ¿Qué color quiere? (blanco/negro/azul/ninguno)";
    }

    /**
     * Método pedirRegreso hace que el vendedor pida al cliente regresar en otro
     * momento
     * 
     * @return Solicitud al usuario de regresar en otro momento
     */
    public String pedirRegreso() {
        return "Ese color lo tendremos dentro de 2 semanas. Por favor regrese más tarde.";
    }
}