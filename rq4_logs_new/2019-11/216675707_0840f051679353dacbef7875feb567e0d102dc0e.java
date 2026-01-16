/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lp2.telegrammanager.models;

/**
 *
 * @author mathe
 */
public abstract class Printable {
    final String print(){
        return printAttributes() + printRelationships();
    }
    
    abstract String printAttributes();
    
    abstract String printRelationships();
}