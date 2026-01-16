/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package amuse;

/**
 *
 * @author Inti Velasquez
 */
public class FunctionCall {
    public String call;
    
    FunctionCall(String call) {
        this.call = call;
    }
    
    @Override
    public String toString(){
        return "Call: "+this.call;
    }
}