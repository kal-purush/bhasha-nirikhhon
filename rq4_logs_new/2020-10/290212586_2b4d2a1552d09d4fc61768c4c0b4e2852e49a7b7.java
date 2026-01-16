package com.chatbot.permit.municipal.watsonactions;

import com.chatbot.permit.municipal.repository.MapsRepository;
import com.chatbot.permit.municipal.repository.PolygonsRepository;
import com.chatbot.permit.municipal.service.ParsingService;
import com.chatbot.permit.municipal.zones.MapHandler;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class AddressVerification {

    private String city;
    private String state;
    private String apiKey;
    private PolygonsRepository polygonsRepository;
    private MapsRepository mapsRepository;
    private final String MAPQUEST_BASE_URL = "http://www.mapquestapi.com/geocoding/v1/address?key=";
    private ParsingService parsingService;

    public AddressVerification(String city, String state, String apiKey, PolygonsRepository polygonsRepository, MapsRepository mapsRepository, ParsingService parsingService) {
        this.city = city;
        this.state = state;
        this.apiKey = apiKey;
        this.polygonsRepository = polygonsRepository;
        this.mapsRepository = mapsRepository;
        this.parsingService = parsingService;
    }

    /**
     * Makes call to MapQuest Geocode api to get latitude and longitude of address
     *
     * @return
     */
    private LinkedHashMap<String, Object> getAddressInfo(String street1) {
        String mapquestUrl = MAPQUEST_BASE_URL + apiKey + "&location=" + street1 + ", " + city + ", " + state;
        LinkedHashMap<String, Object> addressInfo;

        try {
            // send request for lat and long values from mapquest api
            LinkedHashMap response = (LinkedHashMap) parsingService.parse(mapquestUrl);
            // extract address info from nested return object
            ArrayList<Object> results = (ArrayList<Object>) response.get("results");
            LinkedHashMap topResult = (LinkedHashMap) results.get(0);
            ArrayList<Object> locations = (ArrayList<Object>) topResult.get("locations");
            addressInfo = (LinkedHashMap<String, Object>) locations.get(0);
        } catch (Exception exe) {
            exe.printStackTrace();
            addressInfo = null;
        }

        return addressInfo;
    }

    /**
     * Return true if MapQuest result has highest confidence for being valid address and false if not. Mapquest returns
     * a geoQualityCode value that specifies how exact the return match is. The last three letters are confidence
     * ratings for different areas. A value of AAA means that the result has the highest confidence
     * value for each area.
     *
     * @param geocodeQualityCode
     * @return
     */
    private boolean verifyMapQuestResult(String geocodeQualityCode) {
        return geocodeQualityCode.substring(2).equals("AAA");
    }

    /**
     * Finds and returns id of polygon zone that contains point specified by latitude and longitude values. Returns -1
     * if no zone is found.
     * @param latLng
     * @return
     * @throws Exception
     */
    private int findPolygonZone(LinkedHashMap<String, Double> latLng) throws Exception {
        MapHandler startApp = new MapHandler(polygonsRepository, mapsRepository);

        return startApp.findZones((Double) latLng.get("lng"), (Double) latLng.get("lat"));
    }

    public int verifyAddress(String street1) {
        int polygonZoneID;
        LinkedHashMap<String, Integer> zoneID = new LinkedHashMap<String, Integer>();

        LinkedHashMap<String, Object> addressInfo = getAddressInfo(street1);

        if (addressInfo == null || !(verifyMapQuestResult((String) addressInfo.get("geocodeQualityCode")))) {
            return -1;
        }

        try {
            polygonZoneID = findPolygonZone((LinkedHashMap<String, Double>) addressInfo.get("latLng"));
        } catch (Exception e) {
            e.printStackTrace();
            polygonZoneID = -1;
        }

        return polygonZoneID;
    }
}