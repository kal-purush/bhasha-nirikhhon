package de.longri.serializable;

/**
 * Created by Hoepfner on 16.11.2015.
 */
public class Analyse {

    private final Serializable seri;

    public Analyse(Serializable seri) {
        this.seri = seri;
    }


    public void printAnalyse() {
        System.out.println("NormalStore          = " + getLength(this.seri, new NormalStore()) + " bytes");
        System.out.println("BitStore             = " + getLength(this.seri, new BitStore()) + " bytes");
        System.out.println("ZippedNormalStore    = " + getLength(this.seri, new ZippedNormalStore()) + " bytes");
        System.out.println("ZippedBitStore       = " + getLength(this.seri, new ZippedBitStore()) + " bytes");
        System.out.println("BitStoreZippedString = " + getLength(this.seri, new BitStoreZippedString()) + " bytes");
    }

    private int getLength(Serializable seri, StoreBase store) {
        try {
            seri.serialize(store);
            return store.getArray().length;
        } catch (NotImplementedException e) {
            e.printStackTrace();
        }
        return 0;
    }

}