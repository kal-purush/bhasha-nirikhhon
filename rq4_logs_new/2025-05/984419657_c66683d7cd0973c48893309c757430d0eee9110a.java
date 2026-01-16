package clasesProyecto;

public class Equipo {
	//Atributos
    String nombre;
    int PJ;
    int PG;
    int PE;
    int PP;
    int GF;
    int GC;
    int DG;
    int puntos;
    float valoracion;
    
    //Constructor
    public Equipo(String nombre, float valoracion) {
    	this.nombre = nombre;
        this.valoracion = valoracion;
	}
    
    //Getters
    public String getNombre() {
        return nombre;
    }

    public float getValoracion() {
        return valoracion;
    }

    //Metodos
    public void actualizarDatos(int golesAFavor, int golesEnContra){
        PJ++;
        GF += GF;
        GC += GC;

        if(golesAFavor > golesEnContra){
            PG++;
            puntos =+ 3;
        } else if (golesAFavor == golesEnContra){
            PE++;
            puntos =+ 1;
        } else {
            PP++;
        }
    }

    public void diferenciaGoles(){
        DG = GF - GC;
    }
}