package com.example.getgrarfinalversion;

import java.io.Serializable;
/**
 * @author		ahmad mashal
 * @version	    1.0
 * @since		05/2020
 *
 * this java class allows to create a new Item named LatAndLng. Used to save the latitude and longitude of a location
 *
 */
public class LatAndLng implements Serializable {
    private Double clat,clong,clastlat,clastlong;
    private Double Dlat,Dlong;

    public LatAndLng(){ }
    public LatAndLng(Double clat, Double clong,Double clastlat,Double clastlong ,Double Dlat,Double Dlong ){
        this.clat = clat;
        this.clong = clong;
        this.Dlat = Dlat;
        this.clastlat = clastlat;
        this.clastlong = clastlong;
        this.Dlat = Dlat;
        this.Dlong = Dlong;
    }

    public Double getClat() {
        return clat;
    }

    public void setClat(Double clat) {
        this.clat = clat;
    }

    public Double getClong() {
        return clong;
    }

    public void setClong(Double clong) {
        this.clong = clong;
    }

    public Double getClastlong() {
        return clastlong;
    }

    public void setClastlong(Double clastlong) {
        this.clastlong = clastlong;
    }
    public Double getClastlat() {
        return clastlat;
    }

    public void setClastlat(Double clastlat) {
        this.clastlat = clastlat;
    }

    public Double getDlat() {
        return Dlat;
    }

    public void setDlat(Double dlat) {
        Dlat = dlat;
    }

    public Double getDlong() {
        return Dlong;
    }

    public void setDlong(Double dlong) {
        Dlong = dlong;
    }
}