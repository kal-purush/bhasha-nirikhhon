package com.example.chessforandroid.Piezas;

import com.example.chessforandroid.R;

public class Alfil extends Pieza {
    public Alfil(int x, int y, boolean blancas) {
        this.blancas = blancas;
        this.x = x;
        this.y = y;
        if (blancas) {
            this.drawable = R.drawable.balfil;
        } else {
            this.drawable = R.drawable.nalfil;
        }
    }
}