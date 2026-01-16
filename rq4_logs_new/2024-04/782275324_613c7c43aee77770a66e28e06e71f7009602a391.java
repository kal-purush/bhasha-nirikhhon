package co.edu.uniquindio.poo.Vehiculo;

public abstract class VehiculoDao {

    protected String nombre;
    protected String placa;


    public VehiculoDao(String nombre, String placa){
        this.nombre = nombre;
        this.placa = placa;

    }


}