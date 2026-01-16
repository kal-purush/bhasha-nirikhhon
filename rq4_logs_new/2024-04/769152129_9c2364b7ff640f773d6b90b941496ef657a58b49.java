package Exceptions.SemanticExceptions;

import LexicalAnalyzer.Token;


/**
 * Clase abstracta que define los tipos de errores semánticos
 * de nuestro compilador
 * @author Yeumen Silva
 */
public abstract class SemanticException extends RuntimeException {

    private Token token;

    /**
     * Constructor de nuestra clase
     * @param token con datos de lina, columna, lexema y token
     */

    public SemanticException(Token token){
        super();
        this.token = token;
    }

    /**
     * Método que va a retornar un string con el mensaje de la excepción
     * @return String
     */
    public abstract String getExceptionType();

    /**
     * Devuelve el token
     * @return Token
     */
    public Token getToken() {
        return token;
    }
}