package com.company.model.moveModel;

import com.company.model.ElementTypePair;

import java.util.HashMap;

public class BuffConstant {
    public static BuffConstant shared = new BuffConstant();
    private HashMap<Integer, Double> buffEffectivity = new HashMap<Integer, Double>();

    private BuffConstant(){
        buffEffectivity.put(-4, (double) (2/6));
        buffEffectivity.put(-3, (double) (2/5));
        buffEffectivity.put(-2,(double) (2/4));
        buffEffectivity.put(-1,(double) (2/3));
        buffEffectivity.put(0,1.0);
        buffEffectivity.put(4, (double) (6/2));
        buffEffectivity.put(3, (double) (5/2));
        buffEffectivity.put(2,(double) (4/2));
        buffEffectivity.put(1,(double) (3/2));
    }

    public double getBuffConstant(int key) {
        double dmgBuff = 0;
        try {
            dmgBuff = buffEffectivity.get(key);
        } catch (Exception e){
            dmgBuff = 0;
        } finally {
            return  dmgBuff;
        }
    }

}