package com.capgemeni.railwayBooking.Controller;

import java.util.Collection;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.capgemeni.railwayBooking.Entity.Traininfo;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.shared.Application;

@RestController
@RequestMapping("/booking")
public class BookingController {
	
	@Autowired
	private RestTemplate restTemplate;
	 
	@Autowired
    private EurekaClient eurekaClient;

    @RequestMapping("/traininfobyname")
    public List<Traininfo> traininfobyname(@RequestParam String trainname) {
        Application application = eurekaClient.getApplication("trainService");
        InstanceInfo instanceInfo = application.getInstances().get(0);
        String url = "http://" + instanceInfo.getIPAddr() + ":" + instanceInfo.getPort() + "/" + "train/traininfobyname";
        System.out.println("URL" + url);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        MultiValueMap<String, String> map= new LinkedMultiValueMap<String, String>();
        map.add("trainname", trainname);

        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<MultiValueMap<String, String>>(map, headers);
        List<Traininfo> listTraininfo = (List<Traininfo>) restTemplate.postForObject(url, request, Collection.class);
        return listTraininfo;
    }
    @RequestMapping("/trains")
    public List<Traininfo> trains() {
        Application application = eurekaClient.getApplication("trainService");
        InstanceInfo instanceInfo = application.getInstances().get(0);
        String url = "http://" + instanceInfo.getIPAddr() + ":" + instanceInfo.getPort() + "/" + "train/trains";
        System.out.println("URL" + url);
        List<Traininfo> listTraininfo = (List<Traininfo>) restTemplate.getForObject(url, Collection.class);
        return listTraininfo;
    }
    
    

}