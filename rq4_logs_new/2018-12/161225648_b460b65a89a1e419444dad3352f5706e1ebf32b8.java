package com.dam.asfaltame;

public class ChangeListenerVariable {
    private boolean variable = false;
    private ChangeListener listener;

    public boolean isVariable() {
        return variable;
    }

    public void setVariable(boolean boo) {
        this.variable = boo;
        if (listener != null) listener.onChange();
    }

    public ChangeListener getListener() {
        return listener;
    }

    public void setListener(ChangeListener listener) {
        this.listener = listener;
    }

    public interface ChangeListener {
        void onChange();
    }
}