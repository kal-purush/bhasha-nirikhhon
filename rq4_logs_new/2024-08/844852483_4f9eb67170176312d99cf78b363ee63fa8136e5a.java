package mx.unam.ciencias.modelado.practica1.strategy.concretas.basicos;

import mx.unam.ciencias.modelado.practica1.strategy.interfaces.Ataque;
import mx.unam.ciencias.modelado.practica1.personajes.Personaje;

/**
 * Clase abstracta para la ejecución de la defenza de los personajes.
 * Todas las defenzas funcionan igual, por eso se abstrae esta clase.
 */
public abstract class AtaqueAbstracto implements Ataque{
    /**Personaje que va a atacar. */
    private Personaje personaje;
    /**Daño del ataque. */
    private static final double DANIO = 1;

    /**Constructor de la clase
     * @param personaje un personaje que deberá defenderse.
     */
    public AtaqueAbstracto(Personaje personaje) {
        this.personaje = personaje;
    }

    /**Implementacion del ataque. */
    @Override public String ejecutaAtaque(Personaje objetivo){
        objetivo.recibeDanio(DANIO);
        return personaje.getNombre() + " casteó " + nombreAtaque() " contra " + objetivo.getNombre();
    }

    /**Setter del personaje asociado. */
    public void setPersonaje(Personaje personaje){
        this.personaje = personaje;
    }

    /**Getter del nombre de la defenza. */
    protected abstract String nombreAtaque();
}