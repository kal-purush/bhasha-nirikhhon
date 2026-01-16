package javaejercicio3relaciones;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

/*
 * Realizar una baraja de cartas españolas orientada a objetos. Una carta tiene un número
entre 1 y 12 (el 8 y el 9 no los incluimos) y un palo (espadas, bastos, oros y copas). Esta
clase debe contener un método toString() que retorne el número de carta y el palo. La
baraja estará compuesta por un conjunto de cartas, 40 exactamente.
Las operaciones que podrá realizar la baraja son:
• barajar(): cambia de posición todas las cartas aleatoriamente.
• siguienteCarta(): devuelve la siguiente carta que está en la baraja, cuando no
haya más o se haya llegado al final, se indica al usuario que no hay más cartas.
• cartasDisponibles(): indica el número de cartas que aún se puede repartir.
• darCartas(): dado un número de cartas que nos pidan, le devolveremos ese
número de cartas. En caso de que haya menos cartas que las pedidas, no
devolveremos nada, pero debemos indicárselo al usuario.
• cartasMonton(): mostramos aquellas cartas que ya han salido, si no ha salido
ninguna indicárselo al usuario
• mostrarBaraja(): muestra todas las cartas hasta el final. Es decir, si se saca una
carta y luego se llama al método, este no mostrara esa primera carta
 */

/**
 *
 * @author Fabi
 */
public class Barajas {
    Cartas carta=new Cartas();//carta que vincula con la clase Cartas
    ArrayList <Cartas> barajaespanola=new ArrayList(40);//conjunto de cartas
    int pedidas;
    
    public Barajas() {
    }

    public Barajas(Cartas carta,ArrayList <Cartas> barajaespanola,int pedidas) {
        this.carta = carta;
        this.barajaespanola=barajaespanola;
        this.pedidas=pedidas;
    }

   

    public void setCarta(Cartas carta) {
        this.carta = carta;
    }

    public Cartas getCarta() {
        return carta;
    }

    public void setBarajaespanola(ArrayList<Cartas> barajaespanola) {
        this.barajaespanola = barajaespanola;
    }

    public ArrayList<Cartas> getBarajaespanola() {
        return barajaespanola;
    }

    public void setPedidas(int pedidas) {
        this.pedidas = pedidas;
    }

    public int getPedidas() {
        return pedidas;
    }
    
    
   
   
    // barajar(): cambia de posición todas las cartas aleatoriamente.
  
public void Barajar(ArrayList<Cartas> cartabaraja){
    //System.out.println("LAS CARTAS BARAJADAS NUEVAMENTE SON ");
       Collections.shuffle( cartabaraja);
//        for (Cartas cartas : cartabaraja) {
//            System.out.println(""+cartas);
//        
//    }
}

//siguienteCarta(): devuelve la siguiente carta que está en la baraja, cuando no
//haya más o se haya llegado al final, se indica al usuario que no hay más cartas.

public void siguienteCarta(ArrayList<Cartas> cartabaraja){
           System.out.println("La siguiente carta es");
            for (Cartas cartas : cartabaraja) {
                

                System.out.println("" + cartas);
           
            }
            System.out.println("no hay mas cartas en la baraja");
}



//• cartasDisponibles(): indica el número de cartas que aún se puede repartir.
public void cartasDisponibles(ArrayList<Cartas> cartabaraja){
        System.out.println("Las cartas que aun quedan disponibles son "+cartabaraja.size());
        }




//• darCartas(): dado un número de cartas que nos pidan, le devolveremos ese
//número de cartas. En caso de que haya menos cartas que las pedidas, no
//devolveremos nada, pero debemos indicárselo al usuario. 
public void darCartas(ArrayList<Cartas> cartabaraja){
    Scanner Leer=new Scanner(System.in);
    System.out.println("Indique el numero de cartas que quiere");
    this.pedidas= Leer.nextInt();
    if(pedidas>cartabaraja.size()){
        System.out.println("Hay menos cartas  en el maso que las pedidas");
    }else{
        System.out.println("Las cartas que se le otorgan son " + this.pedidas);
    }
    
}
    


//• cartasMonton(): mostramos aquellas cartas que ya han salido, si no ha salido
//ninguna indicárselo al usuario
public void cartasMonton(ArrayList <Cartas>cartabaraja,int pedidas){
ArrayList <Cartas>cartasEliminadas=new ArrayList();
    
    if(pedidas<cartabaraja.size()){
    for (int i = 0; i < pedidas; i++) {
        
       cartasEliminadas.add(cartabaraja.get(i));//por alguna razon tiene un limite de 20
       cartabaraja.remove(i);
        
       
    }
   
   
    if(cartabaraja.size()==40){
        System.out.println("El maso esta lleno");
    } else{
        
    System.out.println(" Las cartas que ya salieron son "+cartasEliminadas);
    
    }
    
}
}


//• mostrarBaraja(): muestra todas las cartas hasta el final. Es decir, si se saca una
//carta y luego se llama al método, este no mostrara esa primera carta.

public void mostrarBaraja(ArrayList <Cartas>cartabaraja){

    System.out.println(" Las cartas que quedan en el maso son "+ cartabaraja);
}



    @Override
    public String toString() {
        return "Barajas{" + "barajaespanola=" + barajaespanola + '}';
    }

}
    
