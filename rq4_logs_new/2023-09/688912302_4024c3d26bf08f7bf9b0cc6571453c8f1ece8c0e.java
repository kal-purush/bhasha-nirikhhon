package com.desiato.puresynth.models;

import org.puredata.core.PdReceiver;

public class PdReceiverAdapter implements PdReceiver {

    @Override
    public void print(String s) {
        System.out.println(s); // Print messages from Pd.
    }

    @Override
    public void receiveBang(String s) {

    }

    @Override
    public void receiveFloat(String s, float v) {

    }

    @Override
    public void receiveSymbol(String s, String s1) {

    }

    @Override
    public void receiveList(String s, Object... objects) {

    }

    @Override
    public void receiveMessage(String s, String s1, Object... objects) {

    }
}