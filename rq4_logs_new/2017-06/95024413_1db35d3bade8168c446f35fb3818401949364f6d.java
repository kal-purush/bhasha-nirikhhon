package com.usi.API.google_maps;

import com.google.maps.model.LatLng;

import com.usi.model.Coordinate;
import com.usi.model.Elevation;
import com.usi.model.Location;
import com.usi.repository.ElevationRepository;
import org.springframework.beans.factory.annotation.Autowired;

import java.awt.*;
import java.util.ArrayList;

public class ElevationMatrix {
    private Coordinate nW = new Coordinate(47.243913, 6.366221);
    private Coordinate nE = new Coordinate(47.243913, 18.742255);
    private Coordinate SE = new Coordinate(36.170137, 18.742255);
    private Coordinate sW = new Coordinate(36.170137, 6.366221);
    private double magicNumber = 0.008196;
    private double earthRadius = 6.371;
    private MapsServices mapsServices;
    private ElevationRepository elevationRepository;

    @Autowired
    public ElevationMatrix(ElevationRepository elevationRepository) {
        this.mapsServices = new MapsServicesImpl();
        this.elevationRepository = elevationRepository;
    }

    //    private LatLng pointOnLine(double x, double m, double c){
//        double y =  m * x + c;
//        return new LatLng(x, y);
//    }

    public ArrayList<Elevation> createElevationMatrix() throws Exception{
        ArrayList<Elevation> elevations = new ArrayList<>();
        double count = 0;
        for(double i = (this.nW.getLatitude() + this.sW.getLatitude())/2 + 1, j = (this.nE.getLatitude() + this.SE.getLatitude())/2 + 1; i >= this.sW.getLatitude(); i -= this.magicNumber, j -= this.magicNumber) {
//        for(double i = this.nW.getLatitude(), j = this.nE.getLatitude(), z = 0; z <3; i -= this.magicNumber, j -= this.magicNumber,  z++) {
            Coordinate start = new Coordinate(i, this.nW.getLongitude());
            Coordinate end = new Coordinate(j, this.nW.getLongitude() + (this.nE.getLongitude() - this.nW.getLongitude()) / 2);
            elevations.addAll(this.mapsServices.getElevation(start, end, 512).getContent());
            start.setLongitude(end.getLongitude());
            end.setLongitude(this.nE.getLongitude());
            elevations.addAll(this.mapsServices.getElevation(start, end, 512).getContent());
            count++;
        }
        elevationRepository.save(elevations);
        return elevations;
    }

    public double haversine(Coordinate start, Coordinate end){
        double lat1 = Math.toRadians(start.getLatitude());
        double lat2 = Math.toRadians(end.getLatitude());
        double deltaLat = Math.toRadians(lat2 - lat1);
        double deltaLng = Math.toRadians(end.getLongitude() - start.getLongitude());
        double a = Math.pow(Math.sin(deltaLat/2), 2) + Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(deltaLng/2), 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return this.earthRadius * c;
    }


//    public ArrayList<Double> getLatitudes(){
//        ArrayList<Double> latitudes = new ArrayList<>();
//        for(int i = 0; i < this.elevations.size(); ++i){
//            latitudes.add(this.elevations.get(i).getLatLng().lat);
//        }
//        return latitudes;
//    }
//
//    public ArrayList<Double> getLongitudes(){
//        ArrayList<Double> longitudes = new ArrayList<>();
//        for(int i = 0; i < this.elevations.size(); ++i){
//            longitudes.add(this.elevations.get(i).getLatLng().lng);
//        }
//        return longitudes;
//    }
//
//    public ArrayList<Integer> getElevations(){
//        ArrayList<Integer> elevations = new ArrayList<>();
//        for (int i = 0; i< this.elevations.size(); ++i){
//            elevations.add(this.elevations.get(i).getElevation());
//        }
//        return elevations;
//    }

//    public void writeToFile() throws Exception{
//        this.createElevationMatrix();
//        ArrayList<Double> latitudes = this.getLatitudes();
//        ArrayList<Double> longitudes = this.getLongitudes();
//        ArrayList<Integer> elevations = this.getElevations();
//        try{
//            PrintWriter writer = new PrintWriter("Elevation-Matrix.txt", "UTF-8");
//            for(int i = 0; i < this.elevations.size(); ++i) {
//                writer.print(latitudes.get(i).toString() + " ");
//            }
//            writer.println();
//            for(int i = 0; i < this.elevations.size(); ++i) {
//                writer.print(longitudes.get(i).toString() + " ");
//            }
//            writer.close();
//        } catch (IOException e) {
//            System.out.println(e.getMessage());
//        }
//    }

}

