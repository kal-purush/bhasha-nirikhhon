package com.example.service;

import com.example.dto.Cities;
import com.example.web.CitiesUIController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.List;

/**
 * Created by kostya.nikitin on 9/6/2016.
 */
public class CitiesUIServiceImpl implements CitiesUIService {

    final Logger slf4jLog = LoggerFactory.getLogger(CitiesUIController.class);

    private RestTemplate restTemplate = new RestTemplate();

    @Autowired
    private DiscoveryClient discoveryClient;

    private URI getServiceUri(String serviceId) {
        List<ServiceInstance> list = discoveryClient.getInstances(serviceId);
        if (list != null && list.size() > 0 ) {
            return list.get(0).getUri();
        }
        return null;
    }

    @Override
    public List<Cities> getCities() {
        slf4jLog.info("Get from DB all cities");

        URI uri = getServiceUri("a-citiesfromdb-client");

        List<Cities> resultList;

        if (uri != null ) {
            slf4jLog.debug("List cities is not null");
            resultList = restTemplate.getForObject(uri.toString()+"/cities", List.class);
        } else {
            slf4jLog.debug("List cities is null");
            throw new IllegalStateException("Not found service to get data form DB");
        }

        return resultList;
    }
}