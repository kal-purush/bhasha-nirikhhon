package edu.fiuba.algo3.modelo;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.format.TextStyle;
import java.util.Locale;

public class Reloj {

    LocalDateTime tiempoActual;
    LocalDateTime tiempoFinal;
    
    public Reloj(){
        this.tiempoActual = LocalDateTime.of(2021, Month.NOVEMBER, 22, 7, 0, 0);
        this.tiempoFinal = LocalDateTime.of(2021, Month.NOVEMBER, 28, 17, 0, 0);
    }

    public void pasarHoras(int horas){
        this.tiempoActual = tiempoActual.plusHours(horas);
    }
    
    public boolean quedaTiempo(){
        return (tiempoActual.isBefore(tiempoFinal));
    }

    public String aString(){
        int hora = tiempoActual.getHour();

        String dia = tiempoActual.getDayOfWeek().getDisplayName(TextStyle.FULL, new Locale("es","ES")); // Dia de la semana segun fecha
        dia = dia.substring(0, 1).toUpperCase() + dia.substring(1); // Capitalize

        String str = dia + " " + hora + " hs";
        return str;
    }
}