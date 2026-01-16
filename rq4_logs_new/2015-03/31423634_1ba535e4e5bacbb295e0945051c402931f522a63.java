/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package Entidades;

import java.io.Serializable;

/**
 *
 * @author Pedro
 */
public class Mensagem implements Serializable{
    private String to;
    private String from;
    private String Mensagem;

    /**
     * @return the to
     */
    public String getTo() {
        return to;
    }

    /**
     * @param to the to to set
     */
    public void setTo(String to) {
        this.to = to;
    }

    /**
     * @return the from
     */
    public String getFrom() {
        return from;
    }

    /**
     * @param from the from to set
     */
    public void setFrom(String from) {
        this.from = from;
    }

    /**
     * @return the Mensagem
     */
    public String getMensagem() {
        return Mensagem;
    }

    /**
     * @param Mensagem the Mensagem to set
     */
    public void setMensagem(String Mensagem) {
        this.Mensagem = Mensagem;
    }
    
}