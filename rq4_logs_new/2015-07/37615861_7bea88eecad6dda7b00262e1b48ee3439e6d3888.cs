using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ajedrez.Ajedrez
{
   public class Jugador:IJugador
    {
       private String nombre;
       private Color color;
       private Movimientos movimiento;
    
    
    public Jugador(String nombre, Color color){
        this.nombre = nombre;
        this.color = color;
    }
    
//    @Override
//    public Jugador empezar(){
//        return jugador;
//    }
    
    public void preguntarNombre() {
        
    }
    
    public void preguntarColorPiezas() {
    
    }
   
    public Movimientos pedirMovimiento()
    {
        // Devuelvo un movimiento dummy para poder compilar
        return movimiento;
    }
    
    public Boolean puedeMover()
    {
        // Devuelve verdadero para poder compilar
        return true;
    }
   
    public void anotarPiezaPerdida(Pieza pieza){
        
    }
  }
}