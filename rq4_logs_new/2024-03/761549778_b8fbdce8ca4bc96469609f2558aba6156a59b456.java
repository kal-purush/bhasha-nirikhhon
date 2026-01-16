package tec.poo.ejercicios;

public class ConexionBDSingleton implements ConexionBD{
    private static ConexionBDSingleton instancia;

    private ConexionBDSingleton(){

    }

    public static ConexionBDSingleton getInstance(){
        if(instancia == null){
            instancia = new ConexionBDSingleton();
        }
        return instancia;
    }

    public void conectar(){

    }
    public void desconectar(){

    }

    @Override
    public void ejecutarConsulta(String consulta) {

    }
}