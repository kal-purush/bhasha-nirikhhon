package Exceptions.SemanticExceptions;

import LexicalAnalyzer.Token;

public class InvalidOverrideStatic extends SemanticException{

    public InvalidOverrideStatic(Token token){
        super(token);
    }

    @Override
    public String getExceptionType() {
        return "SOBRESCRITURA INCORRECTA: DECLARACION ESTATICA DIFERENTE";
    }
}