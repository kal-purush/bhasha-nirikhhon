package geohash;

import ch.hsr.geohash.GeoHash;

public class Demo {

	public static void main(String args[]) {
        GeoHash geoHash = GeoHash.withCharacterPrecision(39.915156,  116.403694, 12);
        
        
        System.out.println(geoHash.toString());
        System.out.println(geoHash.toBase32());
        System.out.println(geoHash.getBoundingBox());
        
        for (GeoHash one : geoHash.getAdjacent()) {
            System.out.println(one.toBase32());
        }
        
        //(40.078125,116.3671875) -> (40.0341796875,116.4111328125)
    }

}