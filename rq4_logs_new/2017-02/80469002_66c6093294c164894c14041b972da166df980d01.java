/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package livelyrussell.Scanner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import static java.lang.Character.isDigit;
import static java.lang.Character.isLetter;
import static java.lang.Character.isWhitespace;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import static java.lang.Character.isDigit;
import static java.lang.Character.isLetter;
import static java.lang.Character.isWhitespace;

/**
 *
 * @author Jesse and Rich
 */
public class CMinusScannerb implements Scanner {

    private BufferedReader inFile;
    private File outFile;
    private Token nextToken;
    private final static char EOF = (char) -1;
    private static HashMap map = new HashMap();
    private int lineNo = 1;

    private enum State {
        //Movement
        START, DONE,
        //Midway states
        IN_NUM, IN_ID,
        IN_NOT, IN_ASSIGN,
        IN_GT, IN_LT, IN_SLASH,
        //Comments
        IN_COM1, IN_COM2
    }

    public CMinusScannerb(BufferedReader file) throws IOException {
        
        inFile = file;
        outFile = new File("output.txt");

        map.put("else", Token.TokenType.ELSE);
        map.put("if", Token.TokenType.IF);
        map.put("int", Token.TokenType.INT);
        map.put("return", Token.TokenType.RETURN);
        map.put("void", Token.TokenType.VOID);
        map.put("while", Token.TokenType.WHILE);
        
        nextToken = scanToken();
    }

    @Override
    public Token getNextToken() throws IOException {
        Token returnToken = nextToken;
        if (nextToken.viewType() != Token.TokenType.EOF) {
            nextToken = scanToken();
        }
        return returnToken;
    }

    @Override
    public Token viewNextToken() {
        return nextToken;
    }

    public Token scanToken() throws IOException {

        State state = State.START;
        Token.TokenType type = Token.TokenType.ERROR;
        //Default token type should be changed later on.
        Token token = new Token(type);
        boolean save;
        while (state != State.DONE) {
            save = true;
            //hold on to character so we don't run ahead
            inFile.mark(2);
            //read first char
            char c = (char) inFile.read();

            //switch on char for each case
            switch (state) {
                case START:
                    if (isWhitespace(c)) {
                        save = false;
                        if(c=='\n') {
                            lineNo++;
                        }
                        break;
                    } else if (isDigit(c)) {
                        state = State.IN_NUM;
                    } else if (isLetter(c)) {
                        state = State.IN_ID;
                    } else if (c == '=') {
                        state = State.IN_ASSIGN;
                    } else if (c == '>') {
                        state = State.IN_GT;
                    } else if (c == '<') {
                        state = State.IN_LT;
                    } else if (c == '/') {
                        state = State.IN_SLASH;
                    } else if (c == '!') {
                        state = State.IN_NOT;
                    } else {
                        state = State.DONE;
                        switch (c) {
                            case EOF:
                                save = false;
                                type = Token.TokenType.EOF;
                                break;
                            case '+':
                                type = Token.TokenType.PLUS;
                                break;
                            case '-':
                                type = Token.TokenType.MINUS;
                                break;
                            case '*':
                                type = Token.TokenType.STAR;
                                break;
                            case '(':
                                type = Token.TokenType.LEFTPAREN;
                                break;
                            case ')':
                                type = Token.TokenType.RIGHTPAREN;
                                break;
                            case '{':
                                type = Token.TokenType.LEFTCURLY;
                                break;
                            case '}':
                                type = Token.TokenType.RIGHTCURLY;
                                break;
                            case '[':
                                type = Token.TokenType.LEFTSQUARE;
                                break;
                            case ']':
                                type = Token.TokenType.RIGHTSQUARE;
                                break;
                            case ';':
                                type = Token.TokenType.SEMICOLON;
                                break;
                            case ',':
                                type = Token.TokenType.COMMA;
                                break;
                            default:
                                type = Token.TokenType.ERROR;
                                break;
                        }
                    }
                    break;

                case IN_NUM:
                    if (!isDigit(c)) {
                        inFile.reset();
                        save = false;
                        state = State.DONE;
                        type = Token.TokenType.NUM;
                    }
                    break;
                case IN_ID:
                    if (!isLetter(c)) {
                        inFile.reset();
                        save = false;
                        state = State.DONE;
                        type = Token.TokenType.ID;
                    }
                    break;
                case IN_NOT:
                    if (c == '=') {
                        type = Token.TokenType.NOT_EQUALS;
                        state = State.DONE;
                    } else {
                        inFile.reset();
                        save = false;
                        type = Token.TokenType.ERROR;
                    }
                    break;
                case IN_ASSIGN:
                    if (c == '=') {
                        type = Token.TokenType.EQUAL;
                        state = State.DONE;
                    } else {
                        inFile.reset();
                        save = false;
                        type = Token.TokenType.ASSIGN;
                        state = State.DONE;
                    }
                    break;
                case IN_GT:
                    if (c == '=') {
                        type = Token.TokenType.GREATER_EQUAL;
                        state = State.DONE;
                    } else {
                        inFile.reset();
                        save = false;
                        type = Token.TokenType.GREATER_THAN;
                        state = State.DONE;
                    }
                    break;
                case IN_LT:
                    if (c == '=') {
                        type = Token.TokenType.LESS_EQUAL;
                        state = State.DONE;
                    } else {
                        inFile.reset();
                        save = false;
                        type = Token.TokenType.LESS_THAN;
                        state = State.DONE;
                    }
                    break;
                case IN_SLASH:
                    if (c == '*') {
                        state = State.IN_COM1;
                        save = false;
                        token.setData(null);
                    } else {
                        inFile.reset();
                        save = false;
                        state = State.DONE;
                        type = Token.TokenType.SLASH;
                    }
                    break;
                case IN_COM1:
                    save = false;
                    if (c == '*') {
                        state = State.IN_COM2;
                    }
                    if(c=='\n') {
                            lineNo++;
                        }
                    break;
                case IN_COM2:
                    save = false;
                    switch (c) {
                        case '/':
                            state = State.START;
                            break;
                        case '*':
                            state = State.IN_COM2;
                            break;
                        case '\n':
                            lineNo++;
                        default:
                            state = State.IN_COM1;
                            break;
                    }
                    break;
                case DONE:
                default:
                    System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGH!");
                    state = State.DONE;
                    type = Token.TokenType.ERROR;
                    break;
            }

            if (save) {
                String temp = (String) token.viewData();
                if (temp == null) {
                    temp = "";
                }
                token.setData((Object) (temp + String.valueOf(c)));
            }
            if (state == State.DONE) {
                token.setType(type);
                if (token.viewType() == Token.TokenType.NUM) {
                    Integer holder = Integer.parseInt((String) token.viewData());
                    token.setData(holder);
                } else if (token.viewType() == Token.TokenType.ID) {
                    String holder = (String) token.viewData();
                    type = (Token.TokenType) map.get(holder);
                    if (type != null) {
                        token.setType(type);
                    }
                }
            }
        }
        FileOutputStream fos = new FileOutputStream(outFile, true);
        PrintStream ps = new PrintStream(fos);
        ps.print(lineNo + ": ");
        ps.close();
        printToken(token);
        return token;
    }

    private void printToken(Token token) throws FileNotFoundException, IOException {
        FileOutputStream fos = new FileOutputStream(outFile, true);
        PrintStream ps = new PrintStream(fos);
        switch (token.viewType()) {
            case IF:
            case ELSE:
            case INT:
            case RETURN:
            case VOID:
            case WHILE:
                ps.printf("reserved word: %s\r\n", token.viewData());
                break;
            case PLUS:
                ps.printf("operator: +\r\n");
                break;
            case MINUS:
                ps.printf("operator: -\r\n");
                break;
            case STAR:
                ps.printf("operator: *\r\n");
                break;
            case SLASH:
                ps.printf("operator: /\r\n");
                break;
            case ASSIGN:
                ps.printf("operator: =\r\n");
                break;
            case SEMICOLON:
                ps.printf("\t;\r\n");
                break;
            case COMMA:
                ps.printf("\t,\r\n");
                break;
            case GREATER_THAN:
                ps.printf("operator: >\r\n");
                break;
            case GREATER_EQUAL:
                ps.printf("operator: >=\r\n");
                break;
            case LESS_THAN:
                ps.printf("operator: <\r\n");
                break;
            case LESS_EQUAL:
                ps.printf("operator: <=\r\n");
                break;
            case EQUAL:
                ps.printf("operator: ==\r\n");
                break;
            case NOT_EQUALS:
                ps.printf("operator: !=\r\n");
                break;
            case LEFTPAREN:
                ps.printf("(\r\n");
                break;
            case RIGHTPAREN:
                ps.printf(")\r\n");
                break;
            case LEFTSQUARE:
                ps.printf("[\r\n");
                break;
            case RIGHTSQUARE:
                ps.printf("]\r\n");
                break;
            case LEFTCURLY:
                ps.printf("{\r\n");
                break;
            case RIGHTCURLY:
                ps.printf("}\r\n");
                break;
            case ID:
                ps.printf("ID, name = %s\r\n", token.viewData());
                break;
            case NUM:
                ps.printf("NUM, value = %d\r\n", token.viewData());
                break;
            case ERROR:
                ps.printf("ERROR: %s\r\n", token.viewData());
                break;
            case EOF:
                ps.printf("EOF");
                break;
            default:
                ps.printf("wat");
                break;
        }
        ps.close();
        fos.close();
    }

    public static void main(String args[]) {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("File Name:");
        String filename = new String();
        BufferedReader file;

        try {
            //get filename
            filename = br.readLine();
            //create buffered reader BufferedReader file;
            file = new BufferedReader(new FileReader(filename));

            CMinusScannerb cms = new CMinusScannerb(file);
            
            PrintStream ps = new PrintStream(new FileOutputStream(cms.outFile, false));
            ps.printf("C- Compiler Lex Debug Output: %s\r\n\r\n", filename);
            ps.close();
            
            while (cms.viewNextToken().viewType() != Token.TokenType.EOF) {
                //cms.printToken(cms.getNextToken());
                cms.getNextToken();
            }

        } catch (FileNotFoundException fnfe) {
            System.out.println("The file " + filename + " could not be found.");
        } 
        
        catch (IOException ex) {
            Logger.getLogger(CMinusScannerb.class
                    .getName()).log(Level.SEVERE, null, ex);
        }

    }
}