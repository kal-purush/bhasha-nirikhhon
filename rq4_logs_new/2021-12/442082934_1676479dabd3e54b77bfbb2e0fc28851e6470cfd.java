package com.solr.clientwrapper.domain.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Data
public class MicroservicePost {

    private final Logger log = LoggerFactory.getLogger(MicroservicePost.class);

    private String apiEndpoint;
    private Object requestBodyDTO;


    public MicroservicePostResponse run(){

        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(apiEndpoint);

        ObjectMapper objectMapper = new ObjectMapper();

        MicroservicePostResponse microservicePostResponse=new MicroservicePostResponse();

        try {

            String objJackson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestBodyDTO);
            StringEntity entity = new StringEntity(objJackson);

            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");

            CloseableHttpResponse response = client.execute(httpPost);
            HttpEntity entityResponse = response.getEntity();
            String result = EntityUtils.toString(entityResponse);
            log.debug("RESPONSE: "+ result);

            JSONObject jsonObject= new JSONObject(result );

            microservicePostResponse.setMessage(jsonObject.get("message").toString());
            microservicePostResponse.setStatusCode((int) jsonObject.get("statusCode"));


            client.close();

        } catch (Exception e) {

            e.printStackTrace();
            log.error(e.toString());

            microservicePostResponse.setMessage(e.toString());
            microservicePostResponse.setStatusCode(400);

        }

        return microservicePostResponse;

    }

    @Data
    @NoArgsConstructor
    @EqualsAndHashCode
    public class MicroservicePostResponse{
        private String message;
        private int statusCode;
    }

}