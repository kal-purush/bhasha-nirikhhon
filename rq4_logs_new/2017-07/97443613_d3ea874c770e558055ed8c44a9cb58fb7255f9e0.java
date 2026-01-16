package com.example.robert.test;

import java.lang.reflect.Array;
import java.util.ArrayList;

/**
 * Created by galli_000 on 7/17/2017.
 */
public class Horde {
    public int size;
    public ArrayList<Monster> m;

    public Horde(ArrayList<Monster> Horde){
        m = Horde;
        size = Horde.size();
    }
}