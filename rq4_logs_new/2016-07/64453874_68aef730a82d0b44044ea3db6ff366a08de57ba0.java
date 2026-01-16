package com.qa.demo.api;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qa.demo.utils.api.APIException;
import com.qa.demo.utils.api.APIHelper;
import com.qa.demo.utils.reporting.Log;
import com.sun.jersey.api.client.ClientResponse;

/**
 * Class file to map the json response to the jackson mapper.
 * 
 * @author deenesh
 */
public class JsonResponseMapper {
    private ClientResponse response;
    private NearestStationResponse responseBean;
    private StationIdResponse idResponseBean;

    /**
     * Constructor to initialize the response Object
     * 
     * @param endpoint
     */
    public JsonResponseMapper(String endpoint) {
        this.response = APIHelper.getResponseNoHeader(endpoint);
    }

    public ClientResponse getResonse() {
        return response;
    }

    /**
     * Map the NearestStations response and returns the list of station details in the response.
     * 
     * @return list of station details nearest to the searched city
     * @exception throws
     *                custom exception if the response fails to be parsed. Exception is logged in the test output
     *                report.
     */
    public List<StationDetails> getNearestStations() {
        List<StationDetails> details = new ArrayList<>();

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            // create the response bean object for NearestStationResponse class
            responseBean = objectMapper.readValue(response.getEntity(String.class), NearestStationResponse.class);
            details = responseBean.getDetails();
        } catch (Exception e) {
            Log.info("unable to MAP JSON Object", e);
            throw new APIException("Unable to MAP Json object", e);
        }
        return details;
    }

    /**
     * Maps the station details and returns the list of station details in the response.
     * 
     * @return details of the the searched station id.
     * @exception throws
     *                custom exception if the response fails to be parsed. Exception is logged in the test output
     *                report.
     */
    public StationDetails getStationDetails() {
        StationDetails stationDetails;

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            // create the response bean object for station ids.
            idResponseBean = objectMapper.readValue(response.getEntity(String.class), StationIdResponse.class);
            stationDetails = idResponseBean.getDetails();
        } catch (Exception e) {
            Log.info("unable to MAP JSON Object", e);
            throw new APIException("unable to MAP Json object", e);
        }

        return stationDetails;
    }

}