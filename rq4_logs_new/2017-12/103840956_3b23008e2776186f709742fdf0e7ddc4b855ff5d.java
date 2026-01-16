package lab6;

import lab6.exceptions.LexerException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public final class Lexer {
    
    public static List<Token> tokenize(String input) {
        return new Lexer(input).tokenize();
    }

    private static final String OPERATOR_CHARS = "+-()=<>:;.[]";

    private static final Map<String, TokenType> OPERATORS;
    static {
        OPERATORS = new HashMap<>();
        OPERATORS.put(";", TokenType.SEMI_COLON);
        OPERATORS.put(":", TokenType.COLON);
        OPERATORS.put("+", TokenType.PLUS);
        OPERATORS.put("-", TokenType.MINUS);
        OPERATORS.put("(", TokenType.LEFT_ROUND_BRACKET);
        OPERATORS.put(")", TokenType.RIGHT_ROUND_BRACKET);
        OPERATORS.put(":=", TokenType.ASSIGN);
        OPERATORS.put("<", TokenType.LESS);
        OPERATORS.put(">", TokenType.MORE);
        OPERATORS.put("=", TokenType.EQUALS);
        OPERATORS.put("<>", TokenType.NOT_EQUALS);
        OPERATORS.put("<=", TokenType.LESS_EQUALS);
        OPERATORS.put(">=", TokenType.MORE_EQUALS);
        OPERATORS.put("[", TokenType.LEFT_SQUARE_BRACKET);
        OPERATORS.put("]", TokenType.RIGHT_SQUARE_BRACKET);
        OPERATORS.put("..", TokenType.POINT_POINT);
    }

    private static final Map<String, TokenType> KEYWORDS;
    static {
        KEYWORDS = new HashMap<>();
        KEYWORDS.put("real", TokenType.REAL);
        KEYWORDS.put("longint", TokenType.LONGINT);
        KEYWORDS.put("boolean", TokenType.BOOLEAN);
        KEYWORDS.put("array", TokenType.ARRAY);
        KEYWORDS.put("of", TokenType.OF);
        KEYWORDS.put("true", TokenType.TRUE);
        KEYWORDS.put("false", TokenType.FALSE);
    }

    private final String input;
    private final int length;

    private final List<Token> tokens;
    private final StringBuilder buffer;

    private int pos;
    private int row, col;

    private Lexer(String input) {
        this.input = input;
        length = input.length();

        tokens = new ArrayList<>();
        buffer = new StringBuilder();
        row = col = 1;
    }

    private List<Token> tokenize() {
        while (pos < length) {
            final char current = peek(0);
            if (Character.isDigit(current)) tokenizeNumber();
            else if (isIdentifierStart(current)) tokenizeWord();
            else if (OPERATOR_CHARS.indexOf(current) != -1) {
                tokenizeOperator();
            } else {
                // whitespaces
                next();
            }
        }
        return tokens;
    }

    private void tokenizeNumber() {
        clearBuffer();
        char current = peek(0);
        while (true) {
            if (Character.isLetter(current)) {
                throw error("Variable starts with number");
            }
            if (!Character.isDigit(current)) {
                break;
            }
            buffer.append(current);
            current = next();
        }
        addToken(TokenType.LONGINT, buffer.toString());
    }


    private void tokenizeOperator() {
        char current = peek(0);
        if (current == '/') {
            if (peek(1) == '/') {
                next();
                next();
                tokenizeOneLineComment();
                return;
            } else if (peek(1) == '*') {
                next();
                next();
                tokenizeMultilineComment();
                return;
            }
        }
        clearBuffer();
        while (true) {
            final String text = buffer.toString();
            if (!text.isEmpty() && !OPERATORS.containsKey(text + current)) {
                addToken(OPERATORS.get(text), text);
                return;
            }
            buffer.append(current);
            current = next();
        }
    }

    private void tokenizeWord() {
        clearBuffer();
        buffer.append(peek(0));
        char current = next();
        while (true) {
            if (!isIdentifierPart(current)) {
                break;
            }
            buffer.append(current);
            current = next();
        }

        final String word = buffer.toString();
        addToken(KEYWORDS.getOrDefault(word.toLowerCase(), TokenType.VAR), word.toLowerCase());
    }

    
    private void tokenizeOneLineComment() {
        char current = peek(0);
        while ("\r\n\0".indexOf(current) == -1) {
            current = next();
        }
     }
    
    private void tokenizeMultilineComment() {
        char current = peek(0);
        while (true) {
            if (current == '*' && peek(1) == '/') break;
            if (current == '\0') throw error("Reached end of file while parsing multiline comment");
            current = next();
        }
        next(); // *
        next(); // /
    }

    private boolean isIdentifierStart(char current) {
        return Character.isLetter(current);
    }

    private boolean isIdentifierPart(char current) {
        return Character.isLetterOrDigit(current);
    }
    
    private void clearBuffer() {
        buffer.setLength(0);
    }
    
    private char next() {
        pos++;
        final char result = peek(0);
        if (result == '\n') {
            row++;
            col = 1;
        } else col++;
        return result;
    }
    
    private char peek(int relativePosition) {
        final int position = pos + relativePosition;
        if (position >= length) return '\0';
        return input.charAt(position);
    }

    private void addToken(TokenType type, String text) {
        tokens.add(new Token(type, text, row, col - text.length()));
    }
    
    private LexerException error(String text) {
        return new LexerException(row, col, text);
    }
}