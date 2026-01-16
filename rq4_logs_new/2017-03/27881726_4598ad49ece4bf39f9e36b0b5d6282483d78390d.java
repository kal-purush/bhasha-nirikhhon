/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareok.data.webserv.exceptions;

/**
 *
 * @author Tao Zhao
 */
public class InvalidArticleDataException extends Exception {
    
    /**
     *
     * @param message
     */
    public InvalidArticleDataException(String message){
        super(message);
    }
}