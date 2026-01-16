package com.example.service;

import com.example.dto.Cities;
import com.example.web.CitiesUIController;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.WebApplicationContext;

import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


import static net.minidev.json.JSONArray.toJSONString;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Created by kostya.nikitin on 9/9/2016.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class CitiesUIServiceImplTest {
    final Logger slf4jLog = LoggerFactory.getLogger(CitiesUIServiceImplTest.class);

    @MockBean
    private DiscoveryClient discoveryClient;

    @Autowired
    CitiesUIService citiesUIService;

    @Test
    public void getCities_GoodGetListCities_ReturnListCities() throws Exception {
        ServiceInstance serviceInst = new ServiceInstance() {
            @Override
            public String getServiceId() {
                return null;
            }
            @Override
            public String getHost() {
                return null;
            }
            @Override
            public int getPort() {
                return 0;
            }
            @Override
            public boolean isSecure() {
                return false;
            }
            @Override
            public URI getUri() {
                try {
                    return new URI("http://google.com");
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                    return null;
                }
            }
            @Override
            public Map<String, String> getMetadata() {
                return null;
            }
        } ;
        given(this.discoveryClient.getInstances("a-citiesfromdb-client")
        ).willReturn(Arrays.asList(serviceInst));
        MockRestServiceServer mockServer = MockRestServiceServer.createServer(citiesUIService.getRestTemplate());
        List<Cities> arrCities = Arrays.asList(new  Cities(1, "city1"), new  Cities(2, "city2"));
        mockServer.expect(requestTo("http://google.com/cities")).andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess( toJSONString(arrCities), MediaType.APPLICATION_JSON_UTF8) );

        List<Cities> result = citiesUIService.getCities();

        Assert.assertNotNull(result);
        Assert.assertEquals(2, result.size());
    }

    @Test
    public void getCities_NoServiceWithDataFromDB_ReturnErrorIllegalStateException() throws Exception {
        given(this.discoveryClient.getInstances("a-citiesfromdb-client")
        ).willReturn(null);

        try {
            List<Cities> result = citiesUIService.getCities();

        } catch (Exception e){
            assertThat(e)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Not found service to get data form DB");
        }
    }
}