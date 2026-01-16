package com.chatbot.permit.municipal.watsonactions;

import com.chatbot.permit.municipal.model.Maps;
import com.chatbot.permit.municipal.model.Polygons;
import com.chatbot.permit.municipal.repository.MapsRepository;
import com.chatbot.permit.municipal.repository.PolygonsRepository;
import com.chatbot.permit.municipal.service.JsonParsingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

@RunWith(SpringRunner.class)
@WebMvcTest
class AddressVerificationTest {

    private final String CITY = "Pasadena";
    private final String STATE = "CA";
    private final String API_KEY = "api_key";
    @MockBean
    private JsonParsingService jsonParsingService;
    @MockBean
    private MapsRepository mockMapsRepository;
    @MockBean
    private PolygonsRepository mockPolygonsRepository;


    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        // mock data for maps repository
        List<Integer> zoneIds = Arrays.asList(71);
        given(mockMapsRepository.findMapsDistinctBy()).willReturn(zoneIds);
        given(mockMapsRepository.findByFKPOLYGONID(71)).willReturn(Arrays.asList(
                new Maps(71, -118.171751, 34.181911, -118171751, 34181911),
                new Maps(71, -118.166906, 34.151521, -118166906, 34151521),
                new Maps(71, -118.164063, 34.149659, -118164063, 34149659),
                new Maps(71, -118.163817, 34.164540, -118163817, 34164540)));
        // mock data for polygons repository
        given(mockPolygonsRepository.findById(71)).willReturn(java.util.Optional.of(new Polygons(71, "OS", "OS")));
    }

    @Test
    public void validMapQuestResult() {
        final boolean EXPECTED_RESULT = true;

        AddressVerification av = new AddressVerification(CITY, STATE, API_KEY, mockPolygonsRepository, mockMapsRepository, jsonParsingService);
        boolean result = av.verifyMapQuestResult("L1AAA");

        assertEquals(EXPECTED_RESULT, result);
    }

    @Test
    public void notValidMapQuestResult() {
        final boolean EXPECTED_RESULT = false;

        AddressVerification av = new AddressVerification(CITY, STATE, API_KEY, mockPolygonsRepository, mockMapsRepository, jsonParsingService);
        boolean result = av.verifyMapQuestResult("L1ABA");

        assertEquals(EXPECTED_RESULT, result);
    }

    @Test
    public void successGetAddressInfo() throws JsonProcessingException {
        String street1 = "360 N Arroyo Blvd";

        LinkedHashMap<String, Double> latLon = new LinkedHashMap();
        latLon.put("lng", -118.166029);
        latLon.put("lat", 34.151636);
        LinkedHashMap<String, Object> addressInfo = new LinkedHashMap();
        addressInfo.put("latLng", latLon);
        addressInfo.put("geocodeQualityCode", "L1AAA");
        ArrayList<LinkedHashMap> locations = new ArrayList<LinkedHashMap>();
        locations.add(addressInfo);
        LinkedHashMap<String, ArrayList<LinkedHashMap>> topResult = new LinkedHashMap();
        topResult.put("locations", locations);
        ArrayList<Object> results = new ArrayList<Object>();
        results.add(topResult);
        LinkedHashMap<String, ArrayList<Object>> mapQuestResponse = new LinkedHashMap();
        mapQuestResponse.put("results", results);

        given(jsonParsingService
                .parse(anyString()))
                .willReturn(mapQuestResponse);

        AddressVerification av = new AddressVerification(CITY, STATE, API_KEY, mockPolygonsRepository, mockMapsRepository, jsonParsingService);
        LinkedHashMap<String, Object> result = av.getAddressInfo(street1);

        assertEquals(addressInfo, result);
    }

    @Test
    public void failGetAddressInfo() throws JsonProcessingException {
        String street1 = "360 N Arroyo Blvd";
        given(jsonParsingService
                .parse(anyString()))
                .willReturn(null);

        AddressVerification av = new AddressVerification(CITY, STATE, API_KEY, mockPolygonsRepository, mockMapsRepository, jsonParsingService);
        LinkedHashMap<String, Object> result = av.getAddressInfo(street1);

        assertEquals(null, result);
    }

    @Test
    public void successFindPolygonZone() throws Exception {
        final int EXPECTED_RESULT = 71;

        LinkedHashMap<String, Double> latLng = new LinkedHashMap<String, Double>();
        latLng.put("lng", -118.166029);
        latLng.put("lat", 34.151636);

        AddressVerification av = new AddressVerification(CITY, STATE, API_KEY, mockPolygonsRepository, mockMapsRepository, jsonParsingService);
        int zoneID = av.findPolygonZone(latLng);

        assertEquals(EXPECTED_RESULT, zoneID);
    }

    @Test
    public void failFindPolygonZone() throws Exception {
        final int EXPECTED_RESULT = -1;

        LinkedHashMap<String, Double> latLng = new LinkedHashMap<String, Double>();
        latLng.put("lng", -76.846206);
        latLng.put("lat", 38.912614);

        AddressVerification av = new AddressVerification(CITY, STATE, API_KEY, mockPolygonsRepository, mockMapsRepository, jsonParsingService);
        int zoneID = av.findPolygonZone(latLng);

        assertEquals(EXPECTED_RESULT, zoneID);
    }

    @Test
    public void errorFindPolygonZone() throws Exception {
        final int EXPECTED_RESULT = -1;

        LinkedHashMap<String, Double> latLng = new LinkedHashMap<String, Double>();
        latLng.put("lng", -76.846206);
        latLng.put("lat", 38.912614);

        AddressVerification av = new AddressVerification(CITY, STATE, API_KEY, mockPolygonsRepository, mockMapsRepository, jsonParsingService);
        int zoneID = av.findPolygonZone(latLng);

        assertEquals(EXPECTED_RESULT, zoneID);
    }

    @Test
    public void successVerifyAddress() {
        final int EXPECTED_RESULT = 71;
        String street1 = "360 N Arroyo Blvd";

        LinkedHashMap<String, Double> latLon = new LinkedHashMap();
        latLon.put("lng", -118.166029);
        latLon.put("lat", 34.151636);
        LinkedHashMap<String, Object> addressInfo = new LinkedHashMap();
        addressInfo.put("latLng", latLon);
        addressInfo.put("geocodeQualityCode", "L1AAA");
        ArrayList<LinkedHashMap> locations = new ArrayList<LinkedHashMap>();
        locations.add(addressInfo);
        LinkedHashMap<String, ArrayList<LinkedHashMap>> topResult = new LinkedHashMap();
        topResult.put("locations", locations);
        ArrayList<Object> results = new ArrayList<Object>();
        results.add(topResult);
        LinkedHashMap<String, ArrayList<Object>> mapQuestResponse = new LinkedHashMap();
        mapQuestResponse.put("results", results);

        given(jsonParsingService
                .parse(anyString()))
                .willReturn(mapQuestResponse);

        AddressVerification av = new AddressVerification(CITY, STATE, API_KEY, mockPolygonsRepository, mockMapsRepository, jsonParsingService);
        int zoneID = av.verifyAddress(street1);

        assertEquals(EXPECTED_RESULT, zoneID);
    }

    @Test
    public void failureVerifyAddress() {
        final int EXPECTED_RESULT = -1;
        String street1 = "360 N Arroyo Blvd";

        given(jsonParsingService
                .parse(anyString()))
                .willReturn(null);

        AddressVerification av = new AddressVerification(CITY, STATE, API_KEY, mockPolygonsRepository, mockMapsRepository, jsonParsingService);
        int zoneID = av.verifyAddress(street1);

        assertEquals(EXPECTED_RESULT, zoneID);
    }

    @Test
    public void notPerfectVerifyAddress() {
        final int EXPECTED_RESULT = -1;
        String street1 = "360 N Arroyo Blvd";

        LinkedHashMap<String, Double> latLon = new LinkedHashMap();
        latLon.put("lng", -118.166029);
        latLon.put("lat", 34.151636);
        LinkedHashMap<String, Object> addressInfo = new LinkedHashMap();
        addressInfo.put("latLng", latLon);
        // valid address is L1AAA
        addressInfo.put("geocodeQualityCode", "L1ABA");
        ArrayList<LinkedHashMap> locations = new ArrayList<LinkedHashMap>();
        locations.add(addressInfo);
        LinkedHashMap<String, ArrayList<LinkedHashMap>> topResult = new LinkedHashMap();
        topResult.put("locations", locations);
        ArrayList<Object> results = new ArrayList<Object>();
        results.add(topResult);
        LinkedHashMap<String, ArrayList<Object>> mapQuestResponse = new LinkedHashMap();
        mapQuestResponse.put("results", results);

        given(jsonParsingService
                .parse(anyString()))
                .willReturn(mapQuestResponse);

        AddressVerification av = new AddressVerification(CITY, STATE, API_KEY, mockPolygonsRepository, mockMapsRepository, jsonParsingService);
        int zoneID = av.verifyAddress(street1);

        assertEquals(EXPECTED_RESULT, zoneID);
    }
}