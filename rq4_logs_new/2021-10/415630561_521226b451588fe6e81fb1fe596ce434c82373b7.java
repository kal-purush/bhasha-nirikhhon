package main;

public class IllegalMoveException extends Exception {
    IllegalMoveException(String errorMesage){
        super(errorMesage);
    }

}