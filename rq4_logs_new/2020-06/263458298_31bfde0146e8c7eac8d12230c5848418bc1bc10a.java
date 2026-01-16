package com.xml.soap;

import com.xml.service.AdvertisementService;
import localhost._8085.advertisement_service_schema.AdvertisementRequest;
import localhost._8085.advertisement_service_schema.AdvertisementResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ws.server.endpoint.annotation.Endpoint;
import org.springframework.ws.server.endpoint.annotation.PayloadRoot;
import org.springframework.ws.server.endpoint.annotation.RequestPayload;
import org.springframework.ws.server.endpoint.annotation.ResponsePayload;


@Endpoint
public class AdvertisementEndPoint {
    private static final String NAMESPACE_URI = "http://localhost:8085/advertisement-service-schema";

    @Autowired
    private AdvertisementService advertisementService;

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "advertisementRequest")
    @ResponsePayload
    public AdvertisementResponse createAdvertisement(@RequestPayload AdvertisementRequest request) {
        System.out.println("UOPAAAAAAAAAA :O");
        AdvertisementResponse response = new AdvertisementResponse();

        return response;
    }
}