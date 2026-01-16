/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simuladordeautomatos;

/**
 *
 * @author murilo
 */
public class reg {
    private String ex;
    private estado e;

    public reg(String ex, estado e) {
        this.ex = ex;
        this.e = e;
    }

    public String getEx() {
        return ex;
    }

    public void setEx(String ex) {
        this.ex = ex;
    }

    public estado getE() {
        return e;
    }

    public void setE(estado e) {
        this.e = e;
    }
    
    
}