package org.citybike;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Main {
    public static void main(String[] args) {

        try {
            BufferedReader bufferedReader = new BufferedReader(
                    new FileReader("./src/main/resources/public/data/kaupunkipyoraasemat.csv"));
            while(bufferedReader.readLine() != null) {
                String[] stationList = bufferedReader.readLine().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                Station stationObject = new Station();
                Location locationObject = new Location();
                stationObject.setId(Long.parseLong(stationList[0]));
                stationObject.setFid(Long.parseLong(stationList[1]));
                stationObject.setNimi(stationList[2]);
                stationObject.setNamn(stationList[3]);
                stationObject.setName(stationList[4]);
                stationObject.setOsoite(stationList[5]);
                stationObject.setAdress(stationList[6]);
                stationObject.setKaupunki(stationList[7]);
                stationObject.setStad(stationList[8]);
                stationObject.setOperaattori(stationList[9]);
                stationObject.setKapasiteetit(Integer.parseInt(stationList[10]));
                locationObject.setLatitude(Double.parseDouble(stationList[11]));
                locationObject.setLongitude(Double.parseDouble(stationList[12]));
                stationObject.setLocation(locationObject);
                System.out.println(stationObject);
            }
        } catch (
                IOException e) {
            e.printStackTrace();
        }
    }
}