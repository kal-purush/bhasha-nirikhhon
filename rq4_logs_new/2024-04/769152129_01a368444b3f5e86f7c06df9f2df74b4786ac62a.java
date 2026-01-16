package SemanticAnalyzer;

public class Variable extends Json {

    //Id que identifica la variable
    private String name;

    //Tipo de la variable
    private Struct type;

    //Posición de la variable
    private int pos;

    /**
     * Constructor de clase
     * @param name id de la variable
     * @param type tipo de la variable
     * @param pos posición de la variable
     */

    public Variable(String name, Struct type, int pos){
        this.name = name;
        this.type = type;
        this.pos = pos;
    }
}