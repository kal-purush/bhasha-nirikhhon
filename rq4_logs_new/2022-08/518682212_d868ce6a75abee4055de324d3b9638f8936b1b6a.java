package com.distributedSystems.ReplicationJarProject.Producer;

import com.distributedSystems.ReplicationJarProject.Consumer.ConsumerService;
import com.distributedSystems.ReplicationJarProject.Responses.FillingResponse;
import org.springframework.web.client.RestTemplate;

import java.util.Scanner;

public class ProducerService {


    final String  FILL_JAR_URL = "http://localhost:8100/api/v1/jar/fill-jar";
    final String  SAVE_STATE_URL = "http://localhost:8100/api/v1/jar/save-state";
    final String  RESTORE_STATE_URL = "http://localhost:8100/api/v1/jar/restore-state";


    RestTemplate restTemplate;

    public ProducerService() {
        restTemplate = new RestTemplate();
    }

    public FillingResponse fillJar(){
        FillingResponse response  = restTemplate.getForObject(FILL_JAR_URL, FillingResponse.class);
        System.out.println("Filling Response: \n"+response);
        return  response;
    }

    public void saveState(){
        Object response = restTemplate.getForObject(SAVE_STATE_URL, Object.class);

    }

    public void restoreLastState(){
        Object response = restTemplate.getForObject(RESTORE_STATE_URL, Object.class);

    }
}