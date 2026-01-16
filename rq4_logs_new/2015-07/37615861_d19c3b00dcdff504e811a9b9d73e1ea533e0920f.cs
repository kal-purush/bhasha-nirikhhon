using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ajedrez.Ajedrez
{
   public class Movimiento
    {
       public Ajedrez.Color color;
       public Posicion posActual;
       public Posicion posDestino;
    }

    public  Movimiento()
    {
       this.color="blanco";
       this.posActual=0;
       this.posDestino=0;
    }

    public void Movimiento(Color color, Posicion posicion, Posicion posicion0) {
        this.color=color;
        this.posActual=posicion;
        this.posDestino=posicion0;
        
    }
    public String toString(){
        return "("+this.color+", "+this.posActual+", "+this.posDestino+")";
    }
}