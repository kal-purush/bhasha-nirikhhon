package com.lambdaschool.dogsinitial.controller;

import com.lambdaschool.dogsinitial.exception.ResourceNotFoundException;
import com.lambdaschool.dogsinitial.model.Dog;
import com.lambdaschool.dogsinitial.DogsinitialApplication;
import com.lambdaschool.dogsinitial.model.MessageDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;

@RestController
@RequestMapping("/dogs")
public class DogController
{
    private static final Logger logger = LoggerFactory.getLogger(DogController.class);

    @Autowired
    RabbitTemplate rt;
    // localhost:8080/dogs/dogs
    @GetMapping(value = "/dogs")
    public ResponseEntity<?> getAllDogs()
    {
        logger.info("/data/allemployees accessed");
        MessageDetail message = new MessageDetail("/data/dogs accessed", 7, false);
        rt.convertAndSend(DogsinitialApplication.QUEUE_NAME_HIGH, message);

        return new ResponseEntity<>(DogsinitialApplication.ourDogList.dogList, HttpStatus.OK);
    }

    // localhost:8080/dogs/{id}
    @GetMapping(value = "/{id}")
    public ResponseEntity<?> getDogDetail(@PathVariable long id)
    {
        logger.trace("/data/employee/" + id + " accessed");
        MessageDetail message = new MessageDetail("/data/employee", 1, true);
        rt.convertAndSend(DogsinitialApplication.QUEUE_NAME_LOW, message);

        Dog rtnDog;

        if(DogsinitialApplication.ourDogList.findDog(d -> (d.getId() == id)) == null)
        {
            throw new ResourceNotFoundException("Dog with id " + id + " not found.");
        }else {
             rtnDog = DogsinitialApplication.ourDogList.findDog(d -> (d.getId() == id));
        }
        return new ResponseEntity<>(rtnDog, HttpStatus.OK);
    }

    // localhost:8080/dogs/breeds/{breed}
    @GetMapping(value = "/breeds/{breed}")
    public ResponseEntity<?> getDogBreeds (@PathVariable String breed)
    {
        ArrayList<Dog> rtnDogs = DogsinitialApplication.ourDogList.
                findDogs(d -> d.getBreed().toUpperCase().equals(breed.toUpperCase()));

        if(rtnDogs.size() == 0)
        {
            throw new ResourceNotFoundException("No dogs of breed " + breed + " exist.");
        }
        return new ResponseEntity<>(rtnDogs, HttpStatus.OK);
    }
}