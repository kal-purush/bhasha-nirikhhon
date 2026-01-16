package mx.unam.ciencias.modelado.practica1.personajes;

import java.util.ArrayList;

public class Personaje{
    protected String nombre;
    protected double hp = 100;
    protected Ataque ataque;
    protected Defenza defenza;
    protected ArrayList<Poder> poderes;
    protected Franquicia franquicia;
    public boolean ganador;

    public Personaje(String nombre, Franquicia franquicia){
        this.nombre = nombre;
        this.hp = hp;
        this.franquicia = franquicia;
        poderes = new ArrayList<>();
        ganador = false;
    }

    public Franquicia getFranquicia(){
        return franquicia;
    }

    public void setAtaque(Ataque ataque){
        this.ataque = ataque;
    }

    public void setDefenza(Defenza defenza){
        this.defenza = defenza;
    }

    public void setPoderes(ArrayList<Poder> poderes){
        this.poderes = poderes;
    }

    public void setGanador(boolean ganador){
        this.ganador = ganador;
    }

    public void agregarPoder(Poder poder){
        poderes.add(poder);
    }

    public double getHP(){
        return hp;
    }

    public void recibeDanio(double danio){
        hp -= danio;
    }

    public void recibeCuracion(double curacion){
        hp += curacion;
    }

    public String getNombre(){
        return nombre;
    }

}