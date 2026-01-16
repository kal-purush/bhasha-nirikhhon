package SyntacticAnalyzer;

import Exceptions.LexicalException;
import FileManager.FileManager;
import LexicalAnalyzer.LexicalAnalyzer;
import LexicalAnalyzer.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Clase que será la encargada de llamar al analizador lèxico
 * e imprimir los resultados por consola o llamar a la clase
 * FileManager en caso de que haya que escribir los resultados
 * en un archivo de salida
 * @throws LexicalException
 * @author Yeumen Silva
 */
public class SyntacticExecutor {

    private  final HashMap<String,String> hashMap = new HashMap<>();

    /**
     * Constructor que inicia nuestra tablas hash de palabras reservadas
     * @author Yeumen Silva
     */

    public SyntacticExecutor(){
        startHashTable();
    }

    /**
     * Método que inicia nuestra hashMap agregando todas las
     * palabras reservadas de nuestro lenguaje
     * @author Yeumen Silva
     */

    private void startHashTable(){

        this.hashMap.put("start", "start");
        this.hashMap.put("struct", "struct");
        this.hashMap.put("self", "self");
        this.hashMap.put("st", "st");
        this.hashMap.put("pri", "pri");
        this.hashMap.put("nil", "nil");
        this.hashMap.put("new", "new");
        this.hashMap.put("impl", "impl");
        this.hashMap.put("ret", "ret");
        this.hashMap.put("if", "if");
        this.hashMap.put("else", "else");
        this.hashMap.put("while", "while");;
        this.hashMap.put("true", "true");
        this.hashMap.put("false", "false");
        this.hashMap.put("fn", "fn");
        this.hashMap.put("void", "void");
        this.hashMap.put("Array", "Array");
        this.hashMap.put("Str", "Str");
        this.hashMap.put("Bool", "Bool");
        this.hashMap.put("Int", "Int");
        this.hashMap.put("Char", "Char");

    }


    /**
     * Método que dado un String de entrada crea una instancia de la clase
     * FileMannager, recibe los tokens del analizador léxico y llama
     * a función que guarda los resultados en un archivo de salida
     * o los imprime por consola
     * @param inputPath String con ruta del archivo de entrada
     * @param outputPath String con ruta del archivo de salida
     */
    public void startExecution(String inputPath, String outputPath){
        FileManager fileManager = new FileManager(inputPath, outputPath);

        List<Token> tokens = new ArrayList<Token>();
        String file = fileManager.getInputFile();
        LexicalAnalyzer lexicalAnalyzer = new LexicalAnalyzer(file);

         /*
       Este bloque try/catch es el encargado de ejecutarse siempre
       y cuando no encontremos un error lexico en la ejecución
        */

        try {
            // Crea una lista de tokens utilizando el LexicalAnalyzer
            Token token = lexicalAnalyzer.getNextToken();
            while (! token.getToken().equals(lexicalAnalyzer.getEOF())){

                //Verifico si el lexema esta en la tabla Hash
                if(this.hashMap.containsKey(token.getLexeme())){

                    //Si esta, entonces es una palabra reservada y cambio su token

                    token.setToken(token.getLexeme());
                }

                // Verificamos si no es algunos de los tokens skipeables, si no lo es entonces lo agregamos a la lista de tokens
                if (! token.getToken().equals(lexicalAnalyzer.getBLANK_SPACE()) &&
                        ! token.getToken().equals(lexicalAnalyzer.getTAB()) &&
                        ! token.getToken().equals(lexicalAnalyzer.getCARRIAGE_RETURN()) &&
                        ! token.getToken().equals(lexicalAnalyzer.getNEW_LINE()) &&
                        ! token.getToken().equals(lexicalAnalyzer.getVERTICAL_TAB()) &&
                        !token.getToken().equals(lexicalAnalyzer.getSIMPLE_COMMENT())){

                    tokens.add(token);
                }

                token = lexicalAnalyzer.getNextToken();
            }

            tokens.add(token); // agrega token EOF a la lista de tokens

            if(outputPath == null){

                printResults(tokens); // Imprimo reusltados por consola

            }
            else {
                //Guardo resultados en archivo de salida
                fileManager.saveResults(validTokenFormat(tokens));
            }

        }
        catch (LexicalException exception){


            /*
            Si encuentra un error lexico, llama a metodo
            que sera el encrgado de imprimir el error
             */

            printException(exception);

        }



    }

    /**
     * Método que dado una lista de tokens finales, sera el encargado
     * de escribirlos en consola o de escribirlos en un archivo en el caso
     * de que este se haya dado como parametro de entrada
     * @param tokenList lista con tokens de todo el archivo de entrada
     * @throws IOException si no puedo escribir el archivo
     * @author Yeumen Silva
     */

    private void printResults(List<Token> tokenList) {

        /*
        Llamo a función que convierte las instancias de clase token
        a string con el formato pedido
         */

        List<String> validTokens = validTokenFormat(tokenList);

        //Si no pasaron argumento de path de salida, etonces:

        printByConsole(validTokens);


    }

    /**
     * Método que dado una lista de tokens finales, los convierte a string
     * en el formato indicado por el documento de entrega
     * @param tokenList lista con tokens de todo el archivo de entrada
     * @return Lista con los tokens convertidos en string para imprimir
     * @author Yeumen Silva
     * @author Lucas Moyano
     */

    private List<String> validTokenFormat(List<Token> tokenList){

        //Declaro lista de strings

        List<String> validStrings = new ArrayList<>();

        // Agrego encabezado
        validStrings.add("CORRECTO: ANALISIS LEXICO");
        validStrings.add("| TOKEN | LEXEMA | NUMERO DE LINEA (NUMERO DE COLUMNA) |");

        /*
        Recorro lista de tokens y los guardo en una lista con el
        formato pedido
         */

        for (Token token : tokenList) {

            validStrings.add("| " + token.getToken() + " | " + token.getLexeme() +
                    " | LINEA " + token.getRow() + " (COLUMNA " + token.getColumn() + ") |");

        }

        return validStrings;

    }

    /**
     * Método que dado una lista de strings con formato solicitado,
     * escribe por consola los resultados
     * @param stringTokens lista con strings
     *en formato pedido(token,lexeme,row,column)
     * @author Yeumen Silva
     */
    private void printByConsole(List<String> stringTokens){

        //Imprimo reocrriendo la lista

        for(String tokenString : stringTokens){
            System.out.println(tokenString);
        }

    }


    /**
     * Método que dado un error, lo imprime con el formato pedido
     * @param exception error de tipo léxico
     * @author Yeumen Silva
     */
    private void printException(LexicalException exception){

        Token tokenException = exception.getToken();

        System.out.println("ERROR: LEXICO");
        System.out.println("| NUMERO DE LINEA: | NUMERO DE COLUMNA: | DESCRIPCION: |");

        System.out.println("| Linea " + tokenException.getRow() +
                " | COLUMNA " + tokenException.getColumn() +
                " | " + exception.getExceptionType() + " " + tokenException.getLexeme()  );

    }
}