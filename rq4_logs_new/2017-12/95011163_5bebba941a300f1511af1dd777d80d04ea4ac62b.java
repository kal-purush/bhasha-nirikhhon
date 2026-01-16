/*
 * Copyright (c) 2017. All rights reserved
 */

package fr.techad.edc.client.internal.http;

/**
 * TECH ADVANTAGE
 * All right reserved
 * Created by cochon on 11/12/2017.
 */
public class Error4xxException extends Exception {

    /**
     * Constructor
     *
     * @param message the message to set
     */
    public Error4xxException(String message) {
        super(message);
    }
}