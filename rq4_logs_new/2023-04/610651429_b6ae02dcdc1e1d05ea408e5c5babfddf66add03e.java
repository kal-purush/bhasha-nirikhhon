/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package etu1966.framework;

import java.util.HashMap;

/**
 *
 * @author MAELA
 */
public class ModelView {
    String view;

    public ModelView() {}
    
    public ModelView(String view) {
        this.setView(view);
    }
    public String getView() {
        return view;
    }

    public void setView(String view) {
        this.view = view;
    }
    
    
    
}