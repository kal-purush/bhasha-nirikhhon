package Exceptions;

import LexicalAnalyzer.Token;

import java.util.List;

/**
 * Clase que representa los errores de tipo Sintáctico
 * @author Yeumen Silva
 */
public class SyntacticException extends RuntimeException {

    private Token token;
    private List<String> waiting;
    private String actual;

    /**
     * Constructor de clase SyntacticExcpetion
     * informaciòn del error
     * @author Yeumen Silva
     */

    public SyntacticException(Token token, List<String > waiting,String actual ){

        super();
        this.token = token;
        this.waiting = waiting;
        this.actual = actual;
    }

    /**
     * Método que devuelve el tipo de error
     * @author Yeumen Silva
     */
    public String getExceptionType(){

        String expected = String.join(" o ", this.waiting);
        return "Se esperaba: " + expected + " y llegó: " + this.actual;

    }

    public Token getToken() {
        return token;
    }

}