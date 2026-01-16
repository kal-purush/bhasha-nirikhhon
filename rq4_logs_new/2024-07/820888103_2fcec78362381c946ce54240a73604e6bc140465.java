package com.bnt.TestManagement.Exception;

public class DataAllreadyPresentException extends RuntimeException{
    public DataAllreadyPresentException(String msg){
        super(msg);
    }
}