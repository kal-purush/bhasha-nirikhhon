/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.heigvd.res.lab00;

/**
 *
 * @author dorianekaffo
 */
public class Flute implements IInstrument{
    @Override
    public String play() {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public int getSoundVolume() {
        return 5;
    }

    @Override
    public String getColor() {
        throw new UnsupportedOperationException("Not supported yet."); 
    }
}