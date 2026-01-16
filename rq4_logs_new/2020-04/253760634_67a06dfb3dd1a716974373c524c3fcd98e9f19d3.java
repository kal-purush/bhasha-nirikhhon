package com.example.airbnbapi.api;

import com.example.airbnbapi.model.Media;
import com.example.airbnbapi.model.MediaType;
import com.example.airbnbapi.service.ServiceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RequestMapping("api/v1/media")
@RestController
public class Controller implements CommandLineRunner {

    private final ServiceFactory serviceFactory;


    @Autowired
    public Controller( ServiceFactory serviceFactory) {

        this.serviceFactory = serviceFactory;

    }

    @GetMapping(path = "{type}")
    public  Media[] getAllMedia (@PathVariable("type") MediaType type) {
      return   serviceFactory.getServiceByType(type).getItems();

    }

    @GetMapping(path = "{type}/{id}")

    public Media getGameById (@PathVariable("type") MediaType type, @PathVariable("id") int id){



        return serviceFactory.getServiceByType(type).getMediaById(id);
    }

    @Override
    public void run(String... args) throws Exception {

    }
}