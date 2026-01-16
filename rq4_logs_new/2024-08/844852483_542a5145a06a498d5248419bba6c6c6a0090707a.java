package mx.unam.ciencias.modelado.practica1.strategy.concretas.objetos;

import mx.unam.ciencias.modelado.practica1.personajes.Personaje;
import java.util.List;

public class BombaSmash extends ObjetoAbstracto{

    /**Lista de personajes. */
    private List<Personaje> personajes;
    /**Daño del ataque. */
    private double DANIO = 40;

    public BombaSmash(List<Personaje> personajes){
        this.personajes = personajes;
    }

    /**Implementacion concreta del poder. */
    @Override public void objetoConcreto(Personaje consumidor){
        for(Personaje personaje : personajes){
            if(!(personaje == consumidor)){
                personaje.recibeDanio(40);
            }
        }
    }

    /**Implementación del getter del nombre. */
    @Override public String nombreObjeto(){
        return "Bomba Smash";
    }

    @Override public String descripcion(){
        return "todos los personajes recibieron " + DANIO + " de daño.";
    }
}