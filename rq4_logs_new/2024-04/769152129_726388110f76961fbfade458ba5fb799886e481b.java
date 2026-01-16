package SemanticAnalyzer;

import java.util.Set;

public class Struct extends Json {

    //Id de la clase
    private String name;

    //Clase de la cual hereda
    private String inerithFrom;

    //Lista de atributos
    private Set<Atributes> atributes;

    //Lista de métodos
    private Set<Methods> methods;

    //Indica si la clase fue o no consolidada

    private boolean isConsolidate = false;


    public Struct(String name){
        this.name = name;
    }

    /**
     * Método que retorna el nombre de la clase
     * @return
     */
    public String getName() {
        return name;
    }
}