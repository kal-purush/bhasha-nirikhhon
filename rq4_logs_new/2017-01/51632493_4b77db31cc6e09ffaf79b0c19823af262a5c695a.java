/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareok.data.datahandlers.exceptions;

/**
 *
 * @author Tao Zhao
 */
public class InvalidDoiException extends Exception {
    /**
     *
     * @param message
     */
    public InvalidDoiException(String message){
        super(message);
    }
}