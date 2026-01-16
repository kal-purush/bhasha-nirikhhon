package com.example.demo.AccessPoint;

import lombok.*;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.io.Serializable;

@Entity
@NoArgsConstructor
@Getter
@Setter
@ToString
@AllArgsConstructor
public class Position implements Serializable
{
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;
    private static final double REFERENCE_LAT = 37.99374043917284;
    private static final double REFERENCE_LON = 23.731972208672435;
    private static final double R = 6371e3;
    private double lat;
    private double lon;
    private double x;
    private double y;

    public Position(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public double distance() {
        double f1 = REFERENCE_LAT * Math.PI/180;
        double f2 = REFERENCE_LAT * Math.PI/180;
        double Df = (this.lat - REFERENCE_LAT) * Math.PI/180;
        double Dl = (this.lon - REFERENCE_LON) * Math.PI/180;
        double a = Math.sin(Df/2) * Math.sin(Df/2) +
                Math.cos(f1) * Math.cos(f2) * Math.sin(Dl/2) * Math.sin(Dl/2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        double d = R * c;
        return d;
    }

    public double bearing(){
        double y = Math.sin(this.lon-REFERENCE_LON) * Math.cos(this.lat);
        double x = Math.cos(REFERENCE_LAT)*Math.sin(this.lat) -
                Math.sin(REFERENCE_LAT)*Math.cos(this.lat)*Math.cos(this.lon-REFERENCE_LON);
        double theta = Math.atan2(y, x);
        double brng = (theta * 180/Math.PI + 360) % 360;
        return brng;
    }

    public double getPositionX() {
        double distance = distance();
        double bearing = bearing();
        double x =  distance * Math.sin( bearing * Math.PI /180);
        this.x = x;
        return x;
    }

    public double getPositionY() {
        double distance = distance();
        double bearing = bearing();
        double y = distance * Math.cos( bearing * Math.PI /180);
        this.y = y;
        return y;
    }


}