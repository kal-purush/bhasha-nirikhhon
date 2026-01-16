package com.example.demo.repositories;

import java.io.ByteArrayInputStream;

import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.ssl.SslProperties.Bundles.Watch.File;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
 

@Service
public class AppwriteStorage implements ImageStorageRepository
{
    

    @Override
    public String uploadImage(ByteArrayInputStream image, String imageName) {
         try {

            HttpResponse<JsonNode> response = Unirest.post("https://firebasestorage.googleapis.com/v0/b/datascouting-522d6.appspot.com/o/" + imageName)
                    .header("Content-Type", "image/jpeg")
                    // .field("file", image)
                    // .field("fileName", "AAAAA")
                    .body(image.readAllBytes())
                    .asJson();

            if (response.getStatus() == HttpStatus.OK.value()) {
                return response.getBody().getObject().getString("name");
            } else {
                // Handle error
                return response.getBody().toString();
            }
        } catch (Exception e) {
            // Handle exception
            System.out.println(e.toString());
            return "Error uploading image";
        }
    }
    
}